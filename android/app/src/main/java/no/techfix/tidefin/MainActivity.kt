package no.techfix.tidefin

import android.Manifest
import android.annotation.SuppressLint
import android.app.Activity
import android.content.ComponentName
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.webkit.JavascriptInterface
import android.webkit.WebChromeClient
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.core.content.ContextCompat
import androidx.media3.common.MediaItem
import androidx.media3.common.MediaMetadata
import androidx.media3.common.PlaybackException
import androidx.media3.common.Player
import androidx.media3.session.MediaController
import androidx.media3.session.SessionToken
import com.google.common.util.concurrent.ListenableFuture
import org.json.JSONArray
import org.json.JSONObject
import kotlin.concurrent.thread

/**
 * Shows the Tidefin web app and hands playback to [PlaybackService], so music keeps
 * playing in the background and can be controlled from the lock screen, Bluetooth and the car.
 */
class MainActivity : Activity() {

    private lateinit var web: WebView
    private var base: String? = null
    private var controllerFuture: ListenableFuture<MediaController>? = null
    private var controller: MediaController? = null
    private val main = Handler(Looper.getMainLooper())
    private var pageReady = false

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        web = WebView(this).apply {
            setBackgroundColor(Color.parseColor("#0B0D10"))
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.mediaPlaybackRequiresUserGesture = false
            settings.mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
            settings.cacheMode = WebSettings.LOAD_DEFAULT
            webChromeClient = WebChromeClient()
            webViewClient = object : WebViewClient() {
                override fun onPageFinished(view: WebView, url: String) {
                    pageReady = true
                    pushState()
                }
                override fun onReceivedError(view: WebView, request: WebResourceRequest, error: WebResourceError) {
                    if (request.isForMainFrame) showOffline("Lost connection to Tidefin.")
                }
                override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                    val u = request.url
                    val b = base?.let { Uri.parse(it) }
                    // keep Tidefin inside the app, open everything else in the browser
                    if (b != null && u.host == b.host && u.port == b.port) return false
                    startActivity(Intent(Intent.ACTION_VIEW, u))
                    return true
                }
            }
            addJavascriptInterface(Bridge(), "TidefinNative")
        }
        setContentView(web)

        if (Build.VERSION.SDK_INT >= 33 &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
        connectPlayer()
        connectServer()
    }

    // ---------- server ----------
    private fun connectServer() {
        pageReady = false
        web.loadDataWithBaseURL(null, statusPage("Connecting to Tidefin…", false), "text/html", "utf-8", null)
        thread {
            val found = ServerConfig.resolve(this)
            main.post {
                if (found == null) showOffline("Can't reach Tidefin at home or over Tailscale.")
                else { base = found; web.loadUrl(found) }
            }
        }
    }

    private fun showOffline(msg: String) {
        pageReady = false
        web.loadDataWithBaseURL(null, statusPage(msg, true), "text/html", "utf-8", null)
    }

    private fun statusPage(msg: String, buttons: Boolean) = """
        <html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head>
        <body style="margin:0;height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;
          background:#0b0d10;color:#eef1f5;font-family:sans-serif;text-align:center;padding:24px;box-sizing:border-box">
        <div style="font-size:28px;font-weight:700">Tide<span style="color:#3ee0c5">fin</span></div>
        <p style="color:#9aa2b0;max-width:320px">${msg}</p>
        ${if (buttons) """
          <button onclick="TidefinNative.retry()" style="margin-top:12px;padding:12px 28px;border-radius:24px;border:0;background:#3ee0c5;color:#04201b;font-weight:700;font-size:15px">Try again</button>
          <button onclick="TidefinNative.openSettings()" style="margin-top:12px;padding:12px 28px;border-radius:24px;border:0;background:#1c2027;color:#eef1f5;font-size:15px">Server settings</button>
        """ else ""}
        </body></html>
    """.trimIndent()

    @Deprecated("Deprecated in Java")
    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        if (requestCode == 42) connectServer()
    }

    // ---------- player (background service) ----------
    private fun connectPlayer() {
        val token = SessionToken(this, ComponentName(this, PlaybackService::class.java))
        val f = MediaController.Builder(this, token).buildAsync()
        controllerFuture = f
        f.addListener({
            controller = try { f.get() } catch (e: Exception) { null }
            controller?.addListener(object : Player.Listener {
                override fun onEvents(player: Player, events: Player.Events) { pushState() }
                override fun onPlayerError(error: PlaybackException) {
                    js("{type:'error',message:${JSONObject.quote(error.errorCodeName)}}")
                    controller?.let { if (it.hasNextMediaItem()) { it.seekToNextMediaItem(); it.prepare(); it.play() } }
                }
            })
            pushState()
        }, ContextCompat.getMainExecutor(this))
    }

    private val ticker = object : Runnable {
        override fun run() {
            if (controller?.isPlaying == true) pushState()
            main.postDelayed(this, 500)
        }
    }

    override fun onResume() { super.onResume(); main.post(ticker); pushState() }
    override fun onPause() { super.onPause(); main.removeCallbacks(ticker) }

    /** Tell the web page what the native player is doing. */
    private fun pushState() {
        val c = controller ?: return
        if (!pageReady) return
        val dur = if (c.duration > 0) c.duration else 0
        js("{type:'state',index:${c.currentMediaItemIndex},count:${c.mediaItemCount},playing:${c.isPlaying}," +
            "paused:${!c.playWhenReady},pos:${c.currentPosition},dur:$dur,ended:${c.playbackState == Player.STATE_ENDED}}")
    }

    private fun js(obj: String) {
        if (!pageReady) return
        web.evaluateJavascript("window.tfNative && window.tfNative($obj)", null)
    }

    override fun onBackPressed() {
        if (web.canGoBack()) web.goBack() else moveTaskToBack(true) // keep playing in the background
    }

    override fun onDestroy() {
        main.removeCallbacks(ticker)
        controllerFuture?.let { MediaController.releaseFuture(it) }
        web.destroy()
        super.onDestroy()
    }

    // ---------- JavaScript bridge (called from Tidefin's web player) ----------
    inner class Bridge {
        private fun ui(block: () -> Unit) = main.post(block)

        @JavascriptInterface fun isNative(): Boolean = true
        @JavascriptInterface fun version(): String = BuildConfigCompat.versionName(this@MainActivity)

        /** items: JSON array of {id,url,preview,title,artist,album,art}. */
        @JavascriptInterface
        fun load(json: String, index: Int, repeat: String, posMs: Double) = ui {
            val c = controller ?: return@ui
            val arr = JSONArray(json)
            val items = ArrayList<MediaItem>(arr.length())
            for (i in 0 until arr.length()) {
                val o = arr.getJSONObject(i)
                val meta = MediaMetadata.Builder()
                    .setTitle(o.optString("title"))
                    .setArtist(o.optString("artist"))
                    .setAlbumTitle(o.optString("album"))
                    .setIsPlayable(true)
                if (o.optString("art").isNotBlank()) meta.setArtworkUri(Uri.parse(o.getString("art")))
                val b = MediaItem.Builder()
                    .setMediaId(o.optString("id", i.toString()))
                    .setUri(o.getString("url"))
                    .setMediaMetadata(meta.build())
                if (o.optBoolean("preview")) {
                    b.setClippingConfiguration(MediaItem.ClippingConfiguration.Builder().setEndPositionMs(30_000).build())
                }
                items.add(b.build())
            }
            c.setMediaItems(items, index.coerceIn(0, maxOf(0, items.size - 1)), posMs.toLong().coerceAtLeast(0))
            c.repeatMode = repeatMode(repeat)
            c.prepare()
            c.play()
        }

        @JavascriptInterface fun skipTo(index: Int) = ui { controller?.run { seekTo(index, 0); prepare(); play() } }
        @JavascriptInterface fun play() = ui { controller?.run { if (playbackState == Player.STATE_IDLE) prepare(); play() } }
        @JavascriptInterface fun pause() = ui { controller?.pause() }
        @JavascriptInterface fun next() = ui { controller?.run { if (hasNextMediaItem()) seekToNextMediaItem() } }
        @JavascriptInterface fun prev() = ui { controller?.seekToPrevious() }
        @JavascriptInterface fun seekTo(ms: Double) = ui { controller?.seekTo(ms.toLong()) }
        @JavascriptInterface fun setRepeat(mode: String) = ui { controller?.repeatMode = repeatMode(mode) }
        @JavascriptInterface fun retry() = ui { connectServer() }
        @JavascriptInterface fun openSettings() = ui {
            @Suppress("DEPRECATION")
            startActivityForResult(Intent(this@MainActivity, SettingsActivity::class.java), 42)
        }
        @JavascriptInterface fun server(): String = base ?: ""

        private fun repeatMode(m: String) = when (m) {
            "all" -> Player.REPEAT_MODE_ALL
            "one" -> Player.REPEAT_MODE_ONE
            else -> Player.REPEAT_MODE_OFF
        }
    }
}

/** versionName without generating BuildConfig. */
object BuildConfigCompat {
    fun versionName(a: Activity): String = try {
        a.packageManager.getPackageInfo(a.packageName, 0).versionName ?: ""
    } catch (e: Exception) { "" }
}
