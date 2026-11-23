package no.techfix.tidefin

import android.content.Context
import java.net.HttpURLConnection
import java.net.URL

/** Home (LAN) and remote (Tailscale) addresses of the Tidefin server, and picking the one that answers. */
object ServerConfig {
    private const val PREFS = "tidefin"
    const val DEFAULT_HOME = "http://192.168.1.185:8585"

    fun home(ctx: Context): String =
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString("home", DEFAULT_HOME) ?: DEFAULT_HOME

    fun remote(ctx: Context): String =
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString("remote", "") ?: ""

    fun save(ctx: Context, home: String, remote: String) {
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit()
            .putString("home", normalize(home))
            .putString("remote", normalize(remote))
            .apply()
    }

    fun normalize(input: String): String {
        var s = input.trim().trimEnd('/')
        if (s.isEmpty()) return ""
        if (!s.startsWith("http://") && !s.startsWith("https://")) s = "http://$s"
        // add the default port when only a host/IP was typed
        val hostPart = s.substringAfter("://").substringBefore('/')
        if (!hostPart.contains(':')) s = s.replaceFirst(hostPart, "$hostPart:8585")
        return s
    }

    /** True if Tidefin answers at [base] within [timeoutMs]. Call off the main thread. */
    fun reachable(base: String, timeoutMs: Int): Boolean {
        if (base.isBlank()) return false
        return try {
            val c = URL("$base/api/health").openConnection() as HttpURLConnection
            c.connectTimeout = timeoutMs
            c.readTimeout = timeoutMs
            c.requestMethod = "GET"
            val ok = c.responseCode in 200..299
            c.disconnect()
            ok
        } catch (e: Exception) {
            false
        }
    }

    /** Home address first (fast), then Tailscale. Returns null if neither answers. Call off the main thread. */
    fun resolve(ctx: Context): String? {
        val home = home(ctx)
        val remote = remote(ctx)
        if (reachable(home, 1500)) return home
        if (remote.isNotBlank() && reachable(remote, 5000)) return remote
        if (reachable(home, 4000)) return home // slow Wi-Fi: one more, longer try
        return null
    }
}
