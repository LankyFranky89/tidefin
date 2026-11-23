# Tidefin — set-up on the Ugreen DXP4800 Plus

A web music player for your Jellyfin library. It also searches Tidal, and when you press **↓** on a Tidal song or album, Tidarr downloads it into `/volume1/Media/Music`, where Jellyfin picks it up.

```
Browser / phone (LAN or Tailscale) → Tidefin :8585 ─┬─ Jellyfin :8096   (library, playback)
                                                     ├─ Tidarr   :8484   (downloads + Tidal playback)
                                                     └─ openapi.tidal.com (search, covers)
```

Your API keys stay inside the container and never reach the browser.

## 1. Copy the files to the NAS
In UGOS **Files**, create `docker/tidefin/` on volume 1 and copy the `app` folder into it, so you end up with:
```
/volume1/docker/tidefin/app/server.js
/volume1/docker/tidefin/app/public/index.html
```

## 2. Collect 5 values
| Value | Where to get it |
|---|---|
| NAS LAN IP | UGOS → Control Panel → Network (e.g. `192.168.1.100`) |
| Jellyfin API key | Jellyfin → Dashboard → **API Keys** → `+` → name it "Tidefin" |
| Tidarr API key | Tidarr web UI → settings/configuration dialog → API key |
| Tidal client ID + secret | developer.tidal.com → Dashboard → your app (the one you already made) |

## 3. Check the Tidarr settings (important)
In Tidarr's docker-compose, make sure that:
- `/music` is mapped to `/volume1/Media/Music` (same folder as the Jellyfin library)
- `JELLYFIN_URL` and `JELLYFIN_API_KEY` are set, so Tidarr asks Jellyfin to rescan after each download
- Tidarr is logged in to your Tidal account, and its download quality is set to **Max** in the settings

## 4. Deploy
UGOS **Docker** → **Project** → **Create**. Name it `tidefin`, set the path to `/volume1/docker/tidefin`, paste `docker-compose.yml`, fill in the `CHANGE ME` lines, then **Deploy**.

Open `http://NAS-IP:8585`, or use the NAS's Tailscale IP when you're away from home. The three dots in the top-right corner (Jellyfin / Tidarr / Tidal) should all be green.

## Using it
- **Library**: your Jellyfin albums. Click one to open it, then play it.
- **Search**: shows results from your library and from Tidal. ▶ plays a Tidal song in full through Tidarr. **↓** downloads it (spinner → ✓). Tidal albums have a **Download album** button.
- **Downloads**: the live Tidarr queue, with *Retry failed* and *Clear finished*.
- After a download finishes, the song shows up in Library. If it doesn't, press **Rescan**.

## Troubleshooting
| Symptom | Fix |
|---|---|
| Red Jellyfin dot | Wrong IP or port, or a bad API key. Use the LAN IP, never `localhost`. |
| Red Tidarr dot | Wrong API key, or Tidarr isn't running. |
| Red Tidal dot | Wrong client ID or secret. |
| Tidal song won't play | Check that Tidarr is logged in to Tidal. Downloading still works without playback. |
| Download ✓, but the song isn't in Library | Tidarr's `/music` isn't the Jellyfin folder, or Jellyfin needs a **Rescan**. |
| Logs | Docker → Containers → `tidefin` → Log |

To update, replace the files in `app/` and restart the container. No rebuild is needed.
