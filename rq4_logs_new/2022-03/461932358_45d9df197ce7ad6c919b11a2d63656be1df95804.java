package org.example.bot;

import org.balrial.dao.planificacion.PlanificacionDAO;
import org.balrial.dao.planificacionUsuario.PlanificacionUsuarioDAO;
import org.balrial.dao.usuario.UsuarioDAO;
import org.balrial.factory.DAOFactory;
import org.balrial.factory.DAOFactoryORM;
import org.balrial.model.Planificacion;
import org.balrial.model.PlanificacionUsuario;
import org.balrial.model.Usuario;
import org.telegram.abilitybots.api.objects.Ability;
import org.telegram.abilitybots.api.bot.AbilityBot;


import java.util.List;
import java.util.stream.Collectors;

import static org.telegram.abilitybots.api.objects.Locality.ALL;
import static org.telegram.abilitybots.api.objects.Privacy.PUBLIC;

/**
 * todo: Buscar una forma de hacer configurable el user y el token
 */
public class MyBot extends AbilityBot {

    DAOFactory factory = DAOFactoryORM.getDAOFactory(1);
    PlanificacionDAO planDao = factory.getPlanificacionDAO();
    UsuarioDAO usuarioDAO = factory.getUsuarioDAO();
    RecordatorioManager manager = new RecordatorioManager();


    public MyBot() {
        super("2026788812:AAHVIpeM9Lkd_Ke1Tv5Nis7rCbY_6VBnmtM", "TestBot");
    }


    @Override
    public long creatorId() {
        return 729192255;
    }


    public Ability registrarToken() {
        return Ability.builder()
                .name("registrarse")
                .info("Registrar un token a una cuenta de Telegram")
                .privacy(PUBLIC)
                .locality(ALL)
                .input(0)
                .action(ctx -> {

                    // todo: cambiar esto por un método que busque a un usuario por id de telegram


                    //todo caso de error(?): usuario ya tiene un id
                    //todo caso de error(?): ese id ya tiene un usuario que no es el del token


                    silent.send("Esta cuenta ha sido registrada", ctx.chatId());
                })
                .build();
    }


    public Ability activarRecordatorios() {
        return Ability.builder()
                .name("activar_recordatorios")
                .info("Says hello world!")
                .privacy(PUBLIC)
                .locality(ALL)
                .input(0)
                .action(ctx -> {

                    long id = ctx.user().getId();

                    if (manager.registeredId.contains(id)) {
                        silent.send("Los recordatorios ya están activados para este usuario", ctx.chatId());
                    } else {
                        manager.registeredId.add(ctx.user().getId());
                        System.out.println(manager.registeredId.size());
                        silent.send("Los recordatorios han sido activados", ctx.chatId());

                        manager.doStuff(ctx);
                    }
                })
                .build();
    }


    public Ability desactivarRecordatorios() {
        return Ability.builder()
                .name("desactivar_reccordatorios")
                .info("Says hello world!")
                .privacy(PUBLIC)
                .locality(ALL)
                .input(0)
                .action(ctx -> {
                    silent.send("Desactivar recordatorios", ctx.chatId());
                })
                .build();
    }


    public Ability recuperarPlanes() {
        return Ability.builder()
                .name("planes")
                .info("Recuperas loos planes")
                .privacy(PUBLIC)
                .locality(ALL)
                .input(0)
                .action(ctx -> {

                       /*todo: falta cambiar la mierda actual por un método
                          que devuelva una lista de planificaciones en base al
                        */

                    /*
                    1. Obtener user de esta telegram id
                    2. Obtener los planes de dicho user
                    3. Obtener
                     */

                    silent.send("Desactivar recordatorios", ctx.chatId());
                })
                .build();
    }


    public Usuario userByTelegramId(long id) {

        for (Usuario usuario : usuarioDAO.listar()) {

            if ((long) usuario.getTelegramId() == id) {

                return usuario;
            }
        }

        return null;
    }
}