import Service from '@/service/Service'

export default {
    get: function (email) {
        return Service.api().post(
            "membership",
            {
                email: email
            }
        );
    }
}