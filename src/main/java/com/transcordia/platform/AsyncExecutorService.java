package com.transcordia.platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * All callbacks to entry listeners will be performed asynchronously using this executor
 * service.
 *
 */
public class AsyncExecutorService extends ThreadPoolExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecutorService.class);

    ThreadPoolExecutor _pool;

    public AsyncExecutorService(int threadCount) {
        super(
                1,
                threadCount,
                60, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>()
        );
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable runnable) {
        super.beforeExecute(thread, runnable);
        LOG.debug("Preparing to execute task using thread pool consisting of {} active " +
                "threads in pool of {} threads.", this.getActiveCount(), this.getPoolSize());
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
        LOG.debug("Now completed {} tasks. {} still awaiting execution.",
                this.getCompletedTaskCount(), this.getQueue().size());
        super.afterExecute(runnable, throwable);
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        LOG.debug("Shutting down execution service.");
        // Disable new tasks from being submitted
        pool.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {

                // Cancel currently executing tasks
                LOG.warn("Execution service did not terminate gracefully. Trying hard termination.");
                pool.shutdownNow();

                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    LOG.error("Execution service did not respond to hard termination.");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            LOG.error("Execution service was interrupted while attempting graceful shutdown.");
            pool.shutdownNow();

            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void finalize() {
        shutdownAndAwaitTermination(_pool);
        super.finalize();
    }
}
