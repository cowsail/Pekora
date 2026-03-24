package org.pekora.framework

interface PekoraPlugin {
    fun install(runtime: PekoraFrameworkRuntime)
}
