/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package db.infra;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
    
public class DenormalizedEntity<V> {
    V parentEntity;
    ImmutableMap<String,Object> subEntities;    

    public DenormalizedEntity(V entity, ImmutableMap<String,Object> subEntities) {
        this.parentEntity = entity;
        this.subEntities = subEntities;
    }

    public V getParent() {
        return parentEntity;
    }

    public Map<String, Object> getSubEntities() {
        return subEntities;
    }
    
    
    
    public <T> Optional<T> getSubEntity(Class<? extends T> c,String name) {
        return Optional.ofNullable((T) subEntities.get(name));
    }
    
    public DenormalizedEntity<V> replace(String subEntityName, Object subEntity) {
        HashMap<String, Object> map = new HashMap<>(subEntities);
        if (subEntity==null)
            map.remove(subEntityName);
        else
            map.put(subEntityName, subEntity);
        return new DenormalizedEntity<>(parentEntity,ImmutableMap.copyOf(map));
    }    

    @Override
    public String toString() {
        return "DenormalizedEntity{" + "parentEntity=" + parentEntity + ", subEntities=" + subEntities + '}';
    }    
}
