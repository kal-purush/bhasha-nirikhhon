package DTO;

public class JobDecoderDTO {
    private int jobId;
    private byte[] resultImage;

    public JobDecoderDTO(int jobId, byte[] resultImage) {
        this.jobId = jobId;
        this.resultImage = resultImage;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public byte[] getResultImage() {
        return resultImage;
    }

    public void setResultImage(byte[] resultImage) {
        this.resultImage = resultImage;
    }
}