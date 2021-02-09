/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.morphia.entities;

import com.mongodb.client.MongoCursor;
import dev.morphia.Datastore;
import dev.morphia.TestBase;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dev.morphia.query.experimental.filters.Filters.eq;
import static dev.morphia.query.experimental.updates.UpdateOperators.set;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestEmbeddedValidation extends TestBase {

    @Test
    public void testCreateEntityWithBasicDBList() {
        getMapper().map(TestEntity.class);
        TestEntity entity = new TestEntity();

        List<Map<String, Object>> data = List.of(
            mapOf("data", mapOf("text", "sometext")),
            mapOf("data", mapOf("id", "123")));

        entity.setData(data);
        getDs().save(entity);

        TestEntity testEntity = getDs().find(TestEntity.class)
                                       .filter(eq("_id", entity.getId()))
                                       .first();
        assertEquals(entity, testEntity);

        Query<TestEntity> query = getDs().find(TestEntity.class);
        query.disableValidation();
        query.filter(eq("data.data.id", "123"));

        assertNotNull(query.iterator(new FindOptions().limit(1)).tryNext());
    }

    @Test
    public void testDottedNames() {
        ParentType parentType = new ParentType();
        EmbeddedSubtype embedded = new EmbeddedSubtype();
        embedded.setText("text");
        embedded.setNumber(42L);
        embedded.setFlag(true);
        parentType.setEmbedded(embedded);

        Datastore ds = getDs();
        ds.save(parentType);

        Query<ParentType> query = ds.find(ParentType.class)
                                    .disableValidation()
                                    .filter(eq("embedded.flag", true));

        Assert.assertEquals(parentType, query.iterator(new FindOptions().limit(1)).tryNext());
    }

    @Test
    public void testEmbeddedListQueries() {
        EntityWithListsAndArrays entity = new EntityWithListsAndArrays();
        EmbeddedType fortyTwo = new EmbeddedType(42L, "forty-two");
        entity.setListEmbeddedType(asList(fortyTwo, new EmbeddedType(1L, "one")));
        getDs().save(entity);

        Query<EntityWithListsAndArrays> query = getDs().find(EntityWithListsAndArrays.class)
                                                       .filter(eq("listEmbeddedType.number", 42L));
        MongoCursor<EntityWithListsAndArrays> cursor = query.iterator();

        Assert.assertEquals(fortyTwo, cursor.next().getListEmbeddedType().get(0));
        Assert.assertFalse(cursor.hasNext());

        query.update(set("listEmbeddedType.$.number", 0))
             .execute();

        Assert.assertEquals(0, query.count());

        fortyTwo.setNumber(0L);
        query = getDs().find(EntityWithListsAndArrays.class)
                       .filter(eq("listEmbeddedType.number", 0));
        cursor = query.iterator();

        Assert.assertEquals(fortyTwo, cursor.next().getListEmbeddedType().get(0));
        Assert.assertFalse(cursor.hasNext());

    }

    private Map<String, Object> mapOf(String key, Object value) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    @Entity
    public static class TestEntity {

        @Id
        private ObjectId id;
        private List<Map<String, Object>> data;

        public List<Map<String, Object>> getData() {
            return data;
        }

        public void setData(List<Map<String, Object>> data) {
            this.data = new ArrayList<>();
            this.data.addAll(data);
        }

        public ObjectId getId() {
            return id;
        }

        @Override
        public int hashCode() {
            int result = getId() != null ? getId().hashCode() : 0;
            result = 31 * result + (getData() != null ? getData().hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestEntity)) {
                return false;
            }

            final TestEntity that = (TestEntity) o;

            if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) {
                return false;
            }
            return getData() != null ? getData().equals(that.getData()) : that.getData() == null;

        }
    }
}
