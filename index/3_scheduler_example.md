**The 15 second limitation**

If a job is scheduled to run every 10 seconds, this scheduler completely breaks — it would fire maybe every 15 or 30 seconds depending on tick alignment. The tick interval is the minimum granularity you can support. So this scheduler's finest resolution is "roughly every 15 seconds", meaning it's really only suitable for minute-level cron jobs like `* * * * *`.

**What cloud providers do**

It varies by what they're offering:

- **AWS EventBridge, GCP Cloud Scheduler** — minimum is 1 minute. They don't even try to go finer than that, it's just not the use case.
- **Kubernetes CronJobs** — same, minute-level granularity.
- **More specialized schedulers** like Quartz (Java) or distributed job queues — they tick every second or even sub-second, because they're designed for high-frequency jobs.

The tradeoff is straightforward — ticking every second means:
- More CPU wasted on ticks where nothing fires
- More lock contention on the store from `ListJobs()` every second
- More pressure on Raft if you're submitting frequently

**For your use case**

15 seconds is totally fine for a cron-style scheduler where the finest schedule is `* * * * *` (every minute). You fire within 15 seconds of the scheduled minute, which is acceptable. If you needed second-level granularity you'd shrink the ticker, but you'd also probably rethink using `ListJobs()` on every tick and instead maintain a priority queue sorted by `NextRun` so you're not scanning all jobs every second.