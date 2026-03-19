/**
 * # Serialization — Jackson CBOR Serialization Marker
 *
 * This file defines the [CborSerializable] marker interface used to opt types into
 * Apache Pekko's Jackson CBOR serialization. Pekko Persistence and Pekko Cluster require
 * all messages, events, and state objects that cross serialization boundaries to be
 * associated with a configured serializer.
 *
 * ## How It Works
 *
 * In `application.conf`, the Jackson CBOR serializer is bound to [CborSerializable] (and/or
 * its subtypes) via the `pekko.actor.serialization-bindings` configuration. Any class that
 * implements this interface is automatically serialized using Jackson's CBOR (Concise Binary
 * Object Representation) format, which is compact and efficient for persistent storage and
 * network transport.
 *
 * ## Usage
 *
 * Types that need CBOR serialization (e.g., [RunEvent] subtypes, [RunState], and cluster
 * messages) should implement this interface either directly or through their parent sealed
 * interface/class hierarchy. The [RunEvent] sealed interface in the DSL module already
 * extends this marker, so all event subtypes are automatically included.
 *
 * @see RunEvent
 * @see RunState
 */
package org.pekora.engine

import org.pekora.dsl.*

/**
 * Marker interface for types that should be serialized using Apache Pekko's Jackson CBOR serializer.
 *
 * Implementing this interface signals to Pekko's serialization infrastructure that instances
 * of the implementing class should be serialized and deserialized using the Jackson CBOR
 * binding configured in `application.conf`. This is the recommended serialization approach
 * for Pekko Persistence events, actor state snapshots, and cluster messages.
 *
 * Classes implementing this interface must be serializable by Jackson, meaning they should
 * either be data classes with a primary constructor, or provide appropriate Jackson annotations
 * for custom deserialization.
 *
 * @see RunEvent
 * @see RunState
 */
interface CborSerializable

// Run events already extend RunEvent; we register them for serialization here.
// In application.conf, these types map to the jackson-cbor serializer.
