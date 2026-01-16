import * as Yup from "yup";

export const validateRule = () => {
    return Yup.object().shape({
        requestDetail: Yup.string().required("リクエストの詳細を入力してください"),
        time: Yup.object().shape({
            from: Yup.object().shape({
                hour: Yup.string().required("時間を入力してください"),
                minute: Yup.string().required("分を入力してください"),
                meridiem: Yup.string().required("午前/午後を入力してくださいr"),
            }),
            to: Yup.object().shape({
                hour: Yup.string().required("時間を入力してください"),
                minute: Yup.string().required("分を入力してください"),
                meridiem: Yup.string().required("午前/午後を入力してください"),
            }),
        }),
        salary: Yup.string().required("給与の詳細を入力してください"),
        otherNotes: Yup.string().required("備考の詳細を入力してください"),
    });
};

