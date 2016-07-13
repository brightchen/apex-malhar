package org.apache.apex.malhar.lib.state.spillable;

/**
 * Classes implementing this interface can be used as generators for identifiers for Spillable data structures.
 */
public interface SpillableIdentifierGenerator
{
  /**
   * Generators the next valid identifier for a Spillable data structure.
   * @return A byte array which represents the next valid identifier for a Spillable data structure.
   */
  public byte[] next();

  /**
   * Registers the given identifier with this {@link SpillableIdentifierGenerator}.
   * @param identifier The identifier to register with this {@link SpillableIdentifierGenerator}.
   */
  public void register(byte[] identifier);
}
