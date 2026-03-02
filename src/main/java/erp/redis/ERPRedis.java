package erp.redis;

import erp.AppContext;
import erp.redis.pipeline.PipelineProcessListener;
import erp.redis.pipeline.RedisPipeline;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Method;
import java.util.Set;

public class ERPRedis {
    public static void usePipelineAnnotation() {
        // 配置 Reflections 扫描整个类路径
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .forPackages("") // 扫描所有包
                .setScanners(Scanners.MethodsAnnotated));

        // 获取所有带有 @RedisPipeline 注解的方法
        Set<Method> methods = reflections.getMethodsAnnotatedWith(RedisPipeline.class);
        if (methods.isEmpty()) {
            return;
        }
        PipelineProcessListener pipelineProcessListener = AppContext.getProcessListener(PipelineProcessListener.class);
        if (pipelineProcessListener == null) {
            pipelineProcessListener = new PipelineProcessListener();
            AppContext.registerProcessListener(pipelineProcessListener);
        }
        for (Method method : methods) {
            pipelineProcessListener.addPipelineProcessName(method.getDeclaringClass().getName() + "." + method.getName());
        }
    }
}
