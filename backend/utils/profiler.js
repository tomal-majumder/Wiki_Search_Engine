// utils/profiler.js
const { performance, PerformanceObserver } = require('perf_hooks');

// startSpan measures the time taken for a specific operation
// and stores the result in the provided store array.
// It returns a function that, when called, finalizes the measurement.

function startSpan(label, store) {
  const id = `${label}-${Math.random().toString(36).slice(2)}`;
  performance.mark(`${id}-start`);
  return () => {
    performance.mark(`${id}-end`);
    const e = performance.measure(label, `${id}-start`, `${id}-end`);
    store.push({ name: e.name, duration_ms: +e.duration.toFixed(2) });
  };
}

// sysSnapshot captures the current system memory usage
// and returns it in a structured format.
// It can be used to monitor memory usage over time.
function sysSnapshot() {
  const mem = process.memoryUsage();
  return {
    rss_mb: +(mem.rss / 1048576).toFixed(1),
    heapUsed_mb: +(mem.heapUsed / 1048576).toFixed(1),
    heapTotal_mb: +(mem.heapTotal / 1048576).toFixed(1),
    eventLoopDelay_ms: 0 // can wire up perf_hooks.monitorEventLoopDelay if needed
  };
}

module.exports = { startSpan, sysSnapshot };
