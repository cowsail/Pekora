/**
 * # RunEntityTypeKey — Cluster Sharding Entity Type Registration
 *
 * This file defines the [RunEntityTypeKey] singleton object, which provides the
 * [EntityTypeKey][org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey] used to
 * register and look up [RunEntity] instances within Apache Pekko Cluster Sharding.
 *
 * ## Role in Cluster Sharding
 *
 * Apache Pekko Cluster Sharding requires each entity type to be identified by a unique
 * [EntityTypeKey]. This key is used when:
 * - **Initializing sharding**: The `ClusterSharding.init()` call references this key to
 *   register the [RunEntity] entity type with the shard region.
 * - **Sending messages**: The `ClusterSharding.entityRefFor()` method uses this key together
 *   with an entity ID (the `runId`) to obtain a typed [EntityRef] for message delivery.
 * - **Shard allocation**: The sharding infrastructure uses the key's type name to route
 *   messages to the correct shard and entity across cluster nodes.
 *
 * The key is parameterized with [RunCommand], which is the message type accepted by
 * [RunEntity]. This ensures type-safe message delivery through the sharding infrastructure.
 *
 * @see RunEntity
 * @see RunCommand
 */
package org.pekora.engine

import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey

/**
 * Singleton object holding the [EntityTypeKey] for [RunEntity] cluster sharding registration.
 *
 * This object provides a single, shared [typeKey] instance that is referenced both during
 * shard region initialization and when obtaining entity references for message delivery.
 * The key's type name is derived from [RunEntity.ENTITY_TYPE_KEY].
 *
 * @see RunEntity
 * @see RunEntity.ENTITY_TYPE_KEY
 */
object RunEntityTypeKey {
    /**
     * The [EntityTypeKey] used to register [RunEntity] with Apache Pekko Cluster Sharding.
     *
     * Parameterized with [RunCommand] to ensure that only valid commands can be sent to
     * sharded `RunEntity` instances. The type name string is [RunEntity.ENTITY_TYPE_KEY].
     *
     * @see RunEntity.ENTITY_TYPE_KEY
     */
    val typeKey: EntityTypeKey<RunCommand> = EntityTypeKey.create(
        RunCommand::class.java,
        RunEntity.ENTITY_TYPE_KEY,
    )
}
