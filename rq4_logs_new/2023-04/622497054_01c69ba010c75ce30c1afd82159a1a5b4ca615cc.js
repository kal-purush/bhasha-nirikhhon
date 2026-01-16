const data_Model = require("../models/dataModel")

class dataController {
    static userRegistration = async (req, res) => {
        const { name, dob, email, student_no, is_hosteler, branch, section, contact_no, year, domain, why_Euphony, previous_exp, what_music_is_for_you, meaning_of_Euphony, achievement, have_you_learn_music, favourite_singer_band, type_of_song_you_like} = req.body
        if (name&&dob&&email&&student_no&&is_hosteler&&branch&&section&&contact_no&&year&&domain&&why_Euphony&&previous_exp&&what_music_is_for_you&&meaning_of_Euphony&&achievement&&have_you_learn_music&&favourite_singer_band&&type_of_song_you_like) {
            const isemail = await data_Model.findOne({ email: email })
            if (!isemail) {
                const new_user = data_Model({
                    name: name,
                    dob: new Date(dob),
                    email: email,
                    student_no: student_no,
                    is_hosteler: is_hosteler,
                    branch: branch,
                    section: section,
                    contact_no: contact_no,
                    year: year,
                    domain: domain,
                    why_Euphony: why_Euphony,
                    previous_exp: previous_exp,
                    what_music_is_for_you: what_music_is_for_you,
                    meaning_of_Euphony: meaning_of_Euphony,
                    achievement: achievement,
                    have_you_learn_music: have_you_learn_music,
                    favourite_singer_band: favourite_singer_band,
                    type_of_song_you_like: type_of_song_you_like,
                })
                // userController.sendotp(email,res)
                const save_user = await new_user.save()
                res.status(200).json({
                    message: "Details have been posted successfully!"
                })
            }
            else {
                res.status(403).json({
                    "message": "User already exist!"
                })
            }
        }
        else {
            res.status(403).json({
                "message": "Please enter all the fields"
            })
        }
    }
}
module.exports = dataController