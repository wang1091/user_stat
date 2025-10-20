<!-- In your base HTML template -->
<script src="/static/analytics.js"></script>
<script>
  // Example: mark module opens when user clicks left-side nav
  document.querySelectorAll('[data-module]').forEach(el => {
    el.addEventListener('click', () => {
      window.CheckitAnalytics.track({
        module: el.getAttribute('data-module'),
        action: 'open'
      });
    });
  });
</script>
// /static/analytics.js
(function () {
  const API = "/api/track";
  const CID_NAME = "cid";
  const ONE_YEAR = 365*24*60*60*1000;

  function getCid() {
    const m = document.cookie.match(new RegExp(`(?:^|; )${CID_NAME}=([^;]*)`));
    if (m) return decodeURIComponent(m[1]);
    const cid = "cid_" + cryptoRandom();
    const expires = new Date(Date.now() + ONE_YEAR).toUTCString();
    document.cookie = `${CID_NAME}=${encodeURIComponent(cid)}; expires=${expires}; path=/; SameSite=Lax`;
    return cid;
  }

  function cryptoRandom() {
    if (window.crypto && crypto.getRandomValues) {
      const a = new Uint32Array(4);
      crypto.getRandomValues(a);
      return Array.from(a).map(x => x.toString(16)).join("");
    }
    return (Math.random().toString(16).slice(2) + Date.now().toString(16));
  }

  async function track({ user_id, module, action, meta } = {}) {
    try {
      const body = {
        user_id: user_id || getCid(),
        module: (module || "").toLowerCase(),
        action: (action || "").toLowerCase(),
        meta: meta || {}
      };
      await fetch(API, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
    } catch (e) {
      // swallow errors so UI never breaks
      console.warn("analytics track failed:", e);
    }
  }

  // convenience wrappers for your modules
  const Modules = {
    news: "news",
    earnings: "earnings",
    valuation: "valuation",
    data: "data",
    fda: "fda"
  };

  // expose globally
  window.CheckitAnalytics = { track, Modules };
})();
