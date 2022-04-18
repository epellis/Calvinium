package com.nedellis.calvinium

import io.kotest.core.config.AbstractProjectConfig
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

class ProjectConfig : AbstractProjectConfig() {
    override val projectTimeout: Duration = 10.minutes
}
