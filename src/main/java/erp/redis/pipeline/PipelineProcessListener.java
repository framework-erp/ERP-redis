package erp.redis.pipeline;

import erp.process.ProcessListener;

import java.util.HashSet;
import java.util.Set;

public class PipelineProcessListener implements ProcessListener {
    private Set<String> pipelineProcessNames = new HashSet<>();

    public void addPipelineProcessName(String processName) {
        pipelineProcessNames.add(processName);
    }

    @Override
    public void beforeProcessStart(String processName) {
        if (isPipelineProcess(processName)) {
            PipelineProcessContext ctx = ThreadBoundPipelineProcessContextArray.getProcessContext();
            ctx.clear();
        }
    }

    @Override
    public void afterProcessFinish(String processName) {
        if (isPipelineProcess(processName)) {
            PipelineProcessContext ctx = ThreadBoundPipelineProcessContextArray.getProcessContext();
            ctx.flush();
        }
    }

    @Override
    public void afterProcessFailed(String processName) {

    }

    public boolean isPipelineProcess(String processName) {
        return pipelineProcessNames.contains(processName);
    }
}
