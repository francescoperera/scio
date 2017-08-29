package com.spotify.scio.hollow.api;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class KVDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, KVDelegate {

    private final String key;
    private final String value;
   private KVTypeAPI typeAPI;

    public KVDelegateCachedImpl(KVTypeAPI typeAPI, int ordinal) {
        this.key = typeAPI.getKey(ordinal);
        this.value = typeAPI.getValue(ordinal);
        this.typeAPI = typeAPI;
    }

    public String getKey(int ordinal) {
        return key;
    }

    public boolean isKeyEqual(int ordinal, String testValue) {
        if(testValue == null)
            return key == null;
        return testValue.equals(key);
    }

    public String getValue(int ordinal) {
        return value;
    }

    public boolean isValueEqual(int ordinal, String testValue) {
        if(testValue == null)
            return value == null;
        return testValue.equals(value);
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

    public KVTypeAPI getTypeAPI() {
        return typeAPI;
    }

    public void updateTypeAPI(HollowTypeAPI typeAPI) {
        this.typeAPI = (KVTypeAPI) typeAPI;
    }

}