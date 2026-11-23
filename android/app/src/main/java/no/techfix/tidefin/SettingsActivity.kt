package no.techfix.tidefin

import android.app.Activity
import android.graphics.Color
import android.graphics.drawable.GradientDrawable
import android.os.Bundle
import android.text.InputType
import android.util.TypedValue
import android.view.Gravity
import android.view.ViewGroup.LayoutParams.MATCH_PARENT
import android.view.ViewGroup.LayoutParams.WRAP_CONTENT
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast

/** Simple screen to set the home (Wi-Fi) and Tailscale addresses. */
class SettingsActivity : Activity() {

    private fun dp(v: Int) = TypedValue.applyDimension(TypedValue.COMPLEX_UNIT_DIP, v.toFloat(), resources.displayMetrics).toInt()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val text = Color.parseColor("#EEF1F5")
        val dim = Color.parseColor("#9AA2B0")

        fun label(s: String, size: Float, color: Int) = TextView(this).apply {
            this.text = s; setTextColor(color); setTextSize(TypedValue.COMPLEX_UNIT_SP, size)
        }
        fun field(value: String, hint: String) = EditText(this).apply {
            setText(value); this.hint = hint; setTextColor(text); setHintTextColor(dim)
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_URI
            setSingleLine()
            background = GradientDrawable().apply { cornerRadius = dp(12).toFloat(); setColor(Color.parseColor("#1C2027")) }
            setPadding(dp(14), dp(12), dp(14), dp(12))
        }

        val home = field(ServerConfig.home(this), ServerConfig.DEFAULT_HOME)
        val remote = field(ServerConfig.remote(this), "100.x.y.z:8585 (Tailscale)")

        val col = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(24), dp(40), dp(24), dp(24))
            addView(label("Tidefin settings", 26f, text))
            addView(label("Tidefin tries your home address first and switches to Tailscale automatically when you're away.", 14f, dim).apply { setPadding(0, dp(8), 0, dp(24)) })
            addView(label("Home address (Wi-Fi)", 14f, text))
            addView(home, LinearLayout.LayoutParams(MATCH_PARENT, WRAP_CONTENT).apply { topMargin = dp(6); bottomMargin = dp(18) })
            addView(label("Tailscale address (away from home)", 14f, text))
            addView(remote, LinearLayout.LayoutParams(MATCH_PARENT, WRAP_CONTENT).apply { topMargin = dp(6); bottomMargin = dp(8) })
            addView(label("Find it in the Tailscale app on the NAS, e.g. 100.101.102.103. Port 8585 is added for you.", 12f, dim))
            addView(Button(context).apply {
                this.text = "Save and connect"
                setTextColor(Color.parseColor("#04201B"))
                background = GradientDrawable().apply { cornerRadius = dp(24).toFloat(); setColor(Color.parseColor("#3EE0C5")) }
                setOnClickListener {
                    ServerConfig.save(this@SettingsActivity, home.text.toString(), remote.text.toString())
                    Toast.makeText(this@SettingsActivity, "Saved", Toast.LENGTH_SHORT).show()
                    setResult(RESULT_OK)
                    finish()
                }
            }, LinearLayout.LayoutParams(MATCH_PARENT, dp(52)).apply { topMargin = dp(28) })
            gravity = Gravity.TOP
        }
        setContentView(ScrollView(this).apply { setBackgroundColor(Color.parseColor("#0B0D10")); addView(col) })
    }
}
