/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package events;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
    
public class DenormalizedEntity<V> {
    V parentEntity;
    ImmutableMap<String,Object> subEntities;    

    public DenormalizedEntity(V entity, ImmutableMap<String,Object> subEntities) {
        this.parentEntity = entity;
        this.subEntities = subEntities;
//        this.subEntities = ImmutableMap.copyOf(subEntities.entrySet().stream().
//                collect(Collectors.toMap(sub->new ClassAndName<>(sub.getValue().getClass(),sub.getKey()), sub->sub.getValue())));
    }

    public V getParentEntity() {
        return parentEntity;
    }
    
    public <T> T getSubEntity(Class<? extends T> c,String name) {
        final T obj = (T) subEntities.get(name);
        if (obj==null)
            throw new RuntimeException("no subentity of type "+c.getName()+" in "+this);        
        return obj;
    }
    
    public DenormalizedEntity<V> replace(String subEntityName, Object subEntity) {
        HashMap<String, Object> map = new HashMap<>(subEntities);
        map.put(subEntityName, subEntity);
        return new DenormalizedEntity<>(parentEntity,ImmutableMap.copyOf(map));
    }
/*       
    static class ClassAndName<T> {
        Class<? extends T> c;
        String name;

        public ClassAndName(Class<? extends T> c, String name) {
            this.c = c;
            this.name = name;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 71 * hash + Objects.hashCode(this.c);
            hash = 71 * hash + Objects.hashCode(this.name);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ClassAndName<?> other = (ClassAndName<?>) obj;
            if (!Objects.equals(this.c, other.c)) {
                return false;
            }
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            return true;
        }        
        @Override
        public String toString() {
            return "ClassAndName{" + "c=" + c + ", name=" + name + '}';
        }        
    }
*/
    
}
