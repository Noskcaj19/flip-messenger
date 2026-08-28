plugins {
    id("com.android.application")
}

fun String.asBuildConfigString(): String =
    "\"" + replace("\\", "\\\\").replace("\"", "\\\"") + "\""

val serverUrl = providers.gradleProperty("serverUrl")
    .orElse("https://flip-server.invalid:8443")
val apiToken = providers.gradleProperty("apiToken")
    .orElse("replace-this-with-a-long-random-token")

android {
    namespace = "com.example.tclflipkeytester"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.example.tclflipkeytester"
        minSdk = 30
        targetSdk = 36
        versionCode = 1
        versionName = "0.1"

        buildConfigField("String", "SERVER_URL", serverUrl.get().asBuildConfigString())
        buildConfigField("String", "API_TOKEN", apiToken.get().asBuildConfigString())
    }

    buildFeatures {
        buildConfig = true
    }
}
