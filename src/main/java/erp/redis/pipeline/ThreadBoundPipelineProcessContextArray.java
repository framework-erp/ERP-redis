package erp.redis.pipeline;


public class ThreadBoundPipelineProcessContextArray {
    private static PipelineProcessContext[] threadPipelineProcessContextArray;

    static {
        threadPipelineProcessContextArray = new PipelineProcessContext[1024];
        for (int i = 0; i < threadPipelineProcessContextArray.length; i++) {
            threadPipelineProcessContextArray[i] = new PipelineProcessContext();
        }
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
        for (int i = threadPipelineProcessContextArray.length; i < newArray.length; i++) {
            newArray[i] = new PipelineProcessContext();
        }
        threadPipelineProcessContextArray = newArray;
    }

}
