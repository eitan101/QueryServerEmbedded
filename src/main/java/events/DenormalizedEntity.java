/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package events;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DenormalizedEntity<V> {
    V entity;
    Map<ClassAndName,Object> subEntities;    

    public DenormalizedEntity(V entity, SubEntityBuilder subEntities) {
        this.entity = entity;
        this.subEntities = subEntities.build();
    }

    public V getEntity() {
        return entity;
    }
    
    public <T> T getSubEntity(Class<? extends T> c,String name) {
        final T obj = (T) subEntities.get(new ClassAndName<>(c,name));
        if (obj==null)
            throw new RuntimeException("no subentity of type "+c.getName()+" in "+this);        
        return obj;
    }
    
    
    public static class SubEntityBuilder {
        Map<ClassAndName,Object> map;

        public SubEntityBuilder() {
            map = new HashMap<>();            
        }
        
        public <T> SubEntityBuilder add(T o, String name) {
            map.put(new ClassAndName<>(o.getClass(), name), o);
            return this;            
        }
        
        private Map<ClassAndName,Object> build() {
            return new HashMap<>(map);            
        }
    }
    
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
    
}
