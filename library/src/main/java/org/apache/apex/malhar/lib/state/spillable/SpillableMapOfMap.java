package org.apache.apex.malhar.lib.state.spillable;

/**
 * This class use 
 *
 */
/**
 * This interface define the SpillableByteMap with value of SpillableByteMap
 *
 * @param <PK> Type of parent Key
 * @param <SK> Type of Sub Key
 * @param <V>  Type of Value of Sub Map
 */
public interface SpillableMapOfMap<PK, SK, V> extends Spillable.SpillableByteMap<PK, Spillable.SpillableByteMap<SK, V>>
{
}
