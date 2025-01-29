package erp.redis.pipeline;


public class ThreadBoundPipelineProcessContextArray {
    private static PipelineProcessContext[] threadPipelineProcessContextArray = new PipelineProcessContext[1024];

    public static PipelineProcessContext createPipelineProcessContext() {
        int iTid = (int) Thread.currentThread().getId();
        if (iTid >= threadPipelineProcessContextArray.length) {
            resizeArray(iTid);
        }
        PipelineProcessContext ctx = new PipelineProcessContext();
        threadPipelineProcessContextArray[iTid] = ctx;
        return ctx;
    }

    public static PipelineProcessContext getProcessContext() {
        int iTid = (int) Thread.currentThread().getId();
        if (iTid >= threadPipelineProcessContextArray.length) {
            resizeArray(iTid);
        }
        return threadPipelineProcessContextArray[iTid];
    }

    private static synchronized void resizeArray(int iTid) {
        if (iTid < threadPipelineProcessContextArray.length) {
            return;
        }
        PipelineProcessContext[] newArray = new PipelineProcessContext[iTid * 2];
        System.arraycopy(threadPipelineProcessContextArray, 0, newArray, 0,
                threadPipelineProcessContextArray.length);
        threadPipelineProcessContextArray = newArray;
    }

}
