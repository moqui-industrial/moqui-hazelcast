/*
 * This software is in the public domain under CC0 1.0 Universal plus a 
 * Grant of Patent License.
 * 
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.moqui.hazelcast

import com.hazelcast.cluster.Member
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.scheduledexecutor.IScheduledExecutorService
import com.hazelcast.scheduledexecutor.IScheduledFuture
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler
import com.hazelcast.scheduledexecutor.TaskUtils
import groovy.transform.CompileStatic
import org.moqui.BaseException
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ToolFactory
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.ContextJavaUtil.CustomDistributedScheduledExecutor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable
import java.util.concurrent.ScheduledFuture

/** A factory for getting a CustomDistributedScheduledExecutor (actually Hazelcast IScheduledExecutorService) */
@CompileStatic
class HazelcastScheduledExecutorToolFactory implements ToolFactory<CustomDistributedScheduledExecutor> {
    protected final static Logger logger = LoggerFactory.getLogger(HazelcastScheduledExecutorToolFactory)
    final static String TOOL_NAME = "HazelcastScheduledExecutor"

    protected ExecutionContextFactory ecf = null

    /** Hazelcast Instance */
    protected HazelcastInstance hazelcastInstance = null
    protected CustomDistributedScheduledExecutor scheduledExecutorService = null

    /** Default empty constructor */
    HazelcastScheduledExecutorToolFactory() { }

    @Override
    String getName() { return TOOL_NAME }
    @Override
    void preFacadeInit(ExecutionContextFactory ecf) { }
    @Override
    void init(ExecutionContextFactory ecf) {
        this.ecf = ecf

        ToolFactory<HazelcastInstance> hzToolFactory = ecf.getToolFactory(HazelcastToolFactory.TOOL_NAME)
        if (hzToolFactory == null) {
            throw new BaseException("HazelcastToolFactory not in place, cannot use HazelcastScheduledExecutorToolFactory")
        } else {
            HazelcastInstance hazelcastInstance = hzToolFactory.getInstance()
            scheduledExecutorService = new HazelcastDistributedScheduledExecutor(hazelcastInstance)
        }

        if (ecf instanceof ExecutionContextFactoryImpl)
            ecf.serviceFacade.setDistributedScheduledExecutorService(scheduledExecutorService)
    }

    @Override
    CustomDistributedScheduledExecutor getInstance(Object... parameters) {
        if (scheduledExecutorService == null)
            throw new IllegalStateException("HazelcastScheduledExecutorToolFactory not initialized")

        return scheduledExecutorService
    }

    @Override
    void destroy() {
        // do nothing, Hazelcast shutdown in HazelcastToolFactory
    }

    ExecutionContextFactory getEcf() { return ecf }

    static class HazelcastDistributedScheduledExecutor implements CustomDistributedScheduledExecutor {
        private final IScheduledExecutorService scheduledExecutorService
        private HazelcastInstance hazelcastInstance

        HazelcastDistributedScheduledExecutor() {}

        HazelcastDistributedScheduledExecutor(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null)
                throw new BaseException("HazelcastInstance is null, cannot instantiate HazelcastDistributedScheduledExecutor")

            this.hazelcastInstance = hazelcastInstance
            this.scheduledExecutorService = hazelcastInstance.getScheduledExecutorService("scheduled-service-executor")
        }

        /** Set HazelcastInstance */
        void setHazelcastInstance(HazelcastInstance hazelcastInstance) { this.hazelcastInstance = hazelcastInstance }
        @Override
        <V> Callable<V> decorateTask(String taskName, Callable<V> callable) { return TaskUtils.named(taskName, callable) }
        @Override
        Runnable decorateTask(String taskName, Runnable runnable) { return TaskUtils.named(taskName, runnable) }
        @Override
        ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) { this.scheduledExecutorService.schedule(command, delay, unit) }
        @Override
        <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) { this.scheduledExecutorService.schedule(callable, delay, unit) }
        @Override
        ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) { this.scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit) }
        @Override
        void shutdown() { this.scheduledExecutorService.shutdown() }
        @Override
        ScheduledFuture<?> getScheduledFuture(String taskName) {
            Map<Member, List<IScheduledFuture<Object>>> allScheduledFutures = scheduledExecutorService.getAllScheduledFutures()
            for (List<IScheduledFuture<Object>> scheduledFutureList in allScheduledFutures.values()) {
                IScheduledFuture<Object> found = scheduledFutureList.find { IScheduledFuture<Object> f ->
                    f.getHandler().getTaskName() == taskName
                } as IScheduledFuture<Object>
                if (found != null) return found
            }
            return null
        }
        @Override
        void dispose(ScheduledFuture<?> scheduledFuture) throws IllegalArgumentException {
            if (!(scheduledFuture instanceof IScheduledFuture))
                throw new IllegalArgumentException("You cannot call the dispose method on scheduled future parameter ${scheduledFuture}.")
            if (!scheduledFuture.isCancelled()) scheduledFuture.cancel(false)
            (scheduledFuture as IScheduledFuture).dispose()
        }
    }
    
}
