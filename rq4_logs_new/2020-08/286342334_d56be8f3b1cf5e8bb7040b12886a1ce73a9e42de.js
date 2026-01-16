module.exports = async function(db, { proffyValue, classValue, classScheduleValues }) {
    //inserir dados na tabela de proffys
    const insertedProffy = await db.run(`
        insert into proffys (
            name, 
            avatar, 
            whatsapp, 
            bio
        ) values (
            "${proffyValue.name}", 
            "${proffyValue.avatar}", 
            "${proffyValue.whatsapp}", 
            "${proffyValue.bio}"
        );
    `)

    //inserir dados na tabela classes
    const proffy_id = insertedProffy.lastID
    const insertedClass = await db.run(`
            insert into classes (
                subject,
                cost,
                proffy_id
            ) values (
                "${classValue.subject}", 
                "${classValue.cost}", 
                "${proffy_id}"
            );
    `)

    //Inserir dados na tabela class_schedule
    const class_id = insertedClass.lastID

    const insertedAllClassScheduleValues = classScheduleValues.map((schedule) => {
            return db.run(`
        insert into classes_schedule (
            class_id,
            weekday,
            time_from,
            time_to
        ) values (
            "${class_id}",
            "${schedule.weekday}",
            "${schedule.time_from}",
            "${schedule.time_to}"
        );
    `)
        })
        //Rodar todos os db.run() da class_schedules
    await Promise.all(insertedAllClassScheduleValues) //Espera achar tudo
}