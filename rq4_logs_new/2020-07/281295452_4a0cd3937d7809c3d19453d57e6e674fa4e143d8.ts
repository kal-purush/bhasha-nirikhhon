const RESUME_FILE_ID = '18uTZtOKHPzXdR2MkarfkL1tj2uSlnGzkTx3JFop1pm0';
const DRIVE_GET_URI = `https://www.googleapis.com/drive/v3/files/${RESUME_FILE_ID}?fields=exportLinks&key=AIzaSyCGgXEYNVyAoKbqGnehqHHKUTmJPTP2nq4`;

export const getResumeFilePdfLink = async () => {
    const fileInfo = await fetch(DRIVE_GET_URI).then((response) => {
        if (!response.ok) {
            throw new Error('Failed retrieving file info');
        }

        return response.json();
    });

    const { exportLinks } = fileInfo || {};

    return exportLinks['application/pdf'] || null;
};