import axios from "axios"
export const baseURL = process.env.NEXT_PUBLIC_BASE_URL;
export class MyUploadAdapter {
    loader: any = null;
    getImgName: any = null;
    setDialogOpen: any = null
    imgName: any = null;
    token: any;
    orgId: number = 0;
    addon_ids: number[] = [];
    setUploadProgress: any = null;
    crash: boolean = false;
    setUploading: any = null;
    notification: any = null;
    constructor() {
    }

    setLoader(loader: any) {
        this.loader = loader;
        return this;
    }

    setNotification(notification: any) {
        this.notification = notification;
        return this;
    }

    setUploading_(setUploading: any) {
        this.setUploading = setUploading;
    }

    setUploadProgress_(setUploadProgress: any) {
        this.setUploadProgress = setUploadProgress;
        return this;
    }

    setDialogOpen_(setDialogOpen: any) {
        this.setDialogOpen = setDialogOpen;
        return this;
    }

    setImgName(imgName: any) {
        this.imgName = imgName;
        return this;
    }

    setOther(token: any, orgId: number, addon_ids: number[]) {
        this.token = token;
        this.orgId = orgId;
        this.addon_ids = addon_ids;
        return this;
    }
    setCrash(cr: boolean) {
        this.crash = cr;
        return this;
    }

    async upload() {
        const $this = this;
        // Wait for the image name to be set
        $this.setDialogOpen(true);
        $this.setUploading(true);
        try {
            await new Promise<string>((resolve, reject) => {
                const interval = setInterval(() => {
                    if ($this.imgName) {
                        clearInterval(interval);
                        // $this.setUploading(false);
                        resolve($this.imgName);
                    } else if ($this.crash) {
                        clearInterval(interval);
                        $this.setUploading(false);
                        $this.setCrash(false);
                        reject();//'Could not upload this image.'
                    }
                }, 100);
            });
            return this.loader.file.then(async (file: any | Blob) => {
                const data = new FormData()
                data.append("media", file)
                const genericError = `Couldn't upload file: ${file.name}.`

                return await axios({
                    data,
                    method: "POST",
                    url: baseURL + "v1/s3/media/upload",
                    headers: {
                        "Content-Type": "multipart/form-data",
                        Authorization: `Bearer ${$this.token}`,
                    },
                    params: {
                        organization_id: $this.orgId,
                        addon_ids: $this.addon_ids,
                        name: $this.imgName || file.name
                    },
                    timeout: 50000,
                    onUploadProgress: (progressEvent: { total?: number; loaded: number }) => {
                        if (progressEvent.total) {
                            this.loader.uploadTotal = progressEvent.total
                            this.loader.uploaded = progressEvent.loaded
                            const uploadPercentage = parseInt(
                                Math.round((progressEvent.loaded / progressEvent.total) * 100) + ''
                            )
                            $this.setUploadProgress(uploadPercentage);
                            return uploadPercentage;
                        }
                    },
                })
                    .then(({data}) => {
                        $this.setUploading(false);
                        $this.setImgName('');
                        // alert(JSON.stringify(data?.data?.url))
                        return { default: data?.data?.url, uploaded: true, url: data?.data?.url }
                    })
                    .catch(() => {
                        $this.setUploading(false);
                        $this.setImgName('');
                        return Promise.reject();
                    });
            })
        } catch (error: any) {
            $this.setUploading(false);
            $this.setUploadProgress(0);
            // Show error using Snackbar
            // alert('hi')            // $this.enqueueSnackbar(
            //     error?.message || 'An unexpected error occurred during the upload.',
            //     { variant: 'error' }
            // );
            // return Promise.resolve({ uploaded: false });
            $this.notification(
                error?.message || 'An unexpected error occurred during the upload.',
                { variant: 'error' }
            );
            return Promise.reject(null);
        }
    }

    abort() {
        this.setCrash(false);
        this.setImgName('');
        this.setUploadProgress(0);
        this.setUploading(false);
        return Promise.reject()
    }
}