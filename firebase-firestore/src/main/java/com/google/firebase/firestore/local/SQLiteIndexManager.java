// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.firestore.local;

import static com.google.firebase.firestore.util.Assert.fail;
import static com.google.firebase.firestore.util.Assert.hardAssert;

import androidx.annotation.Nullable;
import com.google.firebase.firestore.core.Bound;
import com.google.firebase.firestore.core.Target;
import com.google.firebase.firestore.index.FirestoreIndexValueWriter;
import com.google.firebase.firestore.index.IndexByteEncoder;
import com.google.firebase.firestore.model.DocumentKey;
import com.google.firebase.firestore.model.FieldIndex;
import com.google.firebase.firestore.model.ResourcePath;
import com.google.firebase.firestore.model.TargetIndexMatcher;
import com.google.firestore.admin.v1.Index;
import com.google.firestore.v1.Value;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A persisted implementation of IndexManager. */
final class SQLiteIndexManager implements IndexManager {
  /**
   * An in-memory copy of the index entries we've already written since the SDK launched. Used to
   * avoid re-writing the same entry repeatedly.
   *
   * <p>This is *NOT* a complete cache of what's in persistence and so can never be used to satisfy
   * reads.
   */
  private final MemoryIndexManager.MemoryCollectionParentIndex collectionParentsCache =
      new MemoryIndexManager.MemoryCollectionParentIndex();

  private final SQLitePersistence db;
  private final LocalSerializer serializer;

  SQLiteIndexManager(SQLitePersistence persistence, LocalSerializer serializer) {
    this.db = persistence;
    this.serializer = serializer;
  }

  @Override
  public void addToCollectionParentIndex(ResourcePath collectionPath) {
    hardAssert(collectionPath.length() % 2 == 1, "Expected a collection path.");

    if (collectionParentsCache.add(collectionPath)) {
      String collectionId = collectionPath.getLastSegment();
      ResourcePath parentPath = collectionPath.popLast();
      db.execute(
          "INSERT OR REPLACE INTO collection_parents "
              + "(collection_id, parent) "
              + "VALUES (?, ?)",
          collectionId,
          EncodedPath.encode(parentPath));
    }
  }

  @Override
  public List<ResourcePath> getCollectionParents(String collectionId) {
    ArrayList<ResourcePath> parentPaths = new ArrayList<>();
    db.query("SELECT parent FROM collection_parents WHERE collection_id = ?")
        .binding(collectionId)
        .forEach(
            row -> {
              parentPaths.add(EncodedPath.decodeResourcePath(row.getString(0)));
            });
    return parentPaths;
  }

  @Override
  public void addFieldIndex(FieldIndex index) {
    int currentMax =
        db.query("SELECT MAX(index_id) FROM index_configuration")
            .firstValue(input -> input.isNull(0) ? 0 : input.getInt(0));

    db.execute(
        "INSERT OR IGNORE INTO index_configuration ("
            + "index_id, "
            + "collection_id, "
            + "index_proto, "
            + "active) VALUES(?, ?, ?, ?)",
        currentMax + 1,
        index.getCollectionId(),
        encodeFieldIndex(index),
        true);
  }

  @Override
  @Nullable
  public Iterable<DocumentKey> getDocumentsMatchingTarget(Target target) {
    ResourcePath parentPath = target.getPath().popLast();
    @Nullable FieldIndex fieldIndex = getMatchingIndex(target);

    if (fieldIndex == null) return null;

    Bound lowerBound = target.getLowerBound(fieldIndex);
    @Nullable Bound upperBound = target.getUpperBound(fieldIndex);

    // Could we do a join here and return the documents?
    ArrayList<DocumentKey> documents = new ArrayList<>();

    if (upperBound != null) {
      List<byte[]> lowerBoundValues = encodeValues(fieldIndex, lowerBound.getPosition(), false);
      List<byte[]> upperBoundValues = encodeValues(fieldIndex, upperBound.getPosition(), false);

      for (byte[] lowerBoundValue : lowerBoundValues) {
        for (byte[] upperBoundValue : upperBoundValues) {
          db.query(
                  "SELECT document_id from field_index WHERE index_id = ? AND index_value "
                      + (lowerBound.isBefore() ? ">=" : ">")
                      + " ? AND index_value "
                      + (upperBound.isBefore() ? "<=" : "<")
                      + " ?")
              .binding(fieldIndex, lowerBoundValue, upperBoundValue)
              .forEach(
                  row -> documents.add(DocumentKey.fromPath(parentPath.append(row.getString(0)))));
        }
      }
    } else {
      List<byte[]> lowerBoundValues = encodeValues(fieldIndex, lowerBound.getPosition(), false);
      for (byte[] lowerBoundValue : lowerBoundValues) {
        db.query(
                "SELECT document_id from field_index WHERE index_id = ? AND index_value "
                    + (lowerBound.isBefore() ? ">=" : ">")
                    + "  ?")
            .binding(fieldIndex, lowerBoundValue)
            .forEach(
                row -> documents.add(DocumentKey.fromPath(parentPath.append(row.getString(0)))));
      }
    }
    return documents;
  }

  private @Nullable FieldIndex getMatchingIndex(Target target) {
    TargetIndexMatcher targetIndexMatcher = new TargetIndexMatcher(target);
    String collectionGroup =
        target.getCollectionGroup() != null
            ? target.getCollectionGroup()
            : target.getPath().getLastSegment();

    List<FieldIndex> activeIndices = new ArrayList<>();

    db.query(
            "SELECT index_id, index_proto FROM index_configuration WHERE collection_group = ? AND active = 1")
        .binding(collectionGroup)
        .forEach(
            row -> {
              try {
                FieldIndex fieldIndex =
                    serializer.decodeFieldIndex(
                        collectionGroup, row.getInt(0), Index.parseFrom(row.getBlob(1)));
                boolean matches = targetIndexMatcher.servedByIndex(fieldIndex);
                if (matches) {
                  activeIndices.add(fieldIndex);
                }
              } catch (InvalidProtocolBufferException e) {
                throw fail("Failed to decode index: " + e);
              }
            });
    ;

    if (activeIndices.isEmpty()) {
      return null;
    }

    // Return the index with the most number of segments
    Collections.sort(
        activeIndices, (i1, i2) -> Integer.compare(i2.segmentCount(), i1.segmentCount()));
    return activeIndices.get(0);
  }

  private List<byte[]> encodeValues(FieldIndex index, List<Value> values, boolean enforceArrays) {
    List<IndexByteEncoder> encoders = new ArrayList<>();
    encoders.add(new IndexByteEncoder());
    encodeValues(index, values, enforceArrays, encoders);
    List<byte[]> result = new ArrayList<>();
    for (IndexByteEncoder encoder : encoders) {
      result.add(encoder.getEncodedBytes());
    }
    return result;
  }

  private void encodeValues(
      FieldIndex index,
      List<Value> values,
      boolean enforceArrays,
      List<IndexByteEncoder> encoders) {
    if (values.isEmpty()) return;
    for (FieldIndex.Segment indexSegment : index) {
      for (IndexByteEncoder indexByteEncoder : new ArrayList<>(encoders)) {
        switch (indexSegment.getKind()) {
          case ORDERED:
            FirestoreIndexValueWriter.INSTANCE.writeIndexValue(
                values.get(0), new IndexByteEncoder());
            break;
          case CONTAINS:
            if (values.get(0).hasArrayValue()) {
              encoders.clear();
              for (Value value : values.get(0).getArrayValue().getValuesList()) {
                IndexByteEncoder clonedEncoder = new IndexByteEncoder();
                clonedEncoder.seed(indexByteEncoder.getEncodedBytes());
                encoders.add(clonedEncoder);
                FirestoreIndexValueWriter.INSTANCE.writeIndexValue(value, new IndexByteEncoder());
              }
            } else if (!enforceArrays) {
              FirestoreIndexValueWriter.INSTANCE.writeIndexValue(
                  values.get(0), new IndexByteEncoder());
            }
            break;
        }
      }
    }
  }

  private byte[] encodeFieldIndex(FieldIndex fieldIndex) {
    return serializer.encodeFieldIndex(fieldIndex).toByteArray();
  }
}
