plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

val runNumber = (System.getenv("GITHUB_RUN_NUMBER") ?: "1").toInt()

android {
    namespace = "no.techfix.tidefin"
    compileSdk = 35

    defaultConfig {
        applicationId = "no.techfix.tidefin"
        minSdk = 26
        targetSdk = 35
        versionCode = runNumber
        versionName = "1.0.$runNumber"
    }

    // Signing key comes from GitHub secrets (never stored in the repo).
    // Without the secrets, CI makes a throwaway key: the APK works, but you'd have to
    // uninstall before installing the next version.
    val ksFile = System.getenv("TIDEFIN_KEYSTORE_FILE")
    signingConfigs {
        create("tidefin") {
            if (ksFile != null && file(ksFile).exists()) {
                storeFile = file(ksFile)
                storePassword = System.getenv("TIDEFIN_KEYSTORE_PASSWORD")
                keyAlias = System.getenv("TIDEFIN_KEY_ALIAS") ?: "tidefin"
                keyPassword = System.getenv("TIDEFIN_KEYSTORE_PASSWORD")
            } else {
                val dbg = signingConfigs.getByName("debug")
                storeFile = dbg.storeFile
                storePassword = dbg.storePassword
                keyAlias = dbg.keyAlias
                keyPassword = dbg.keyPassword
            }
        }
    }

    buildTypes {
        getByName("release") {
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("tidefin")
        }
        getByName("debug") {
            signingConfig = signingConfigs.getByName("tidefin")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}

dependencies {
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.media3:media3-exoplayer:1.5.1")
    implementation("androidx.media3:media3-session:1.5.1")
}
