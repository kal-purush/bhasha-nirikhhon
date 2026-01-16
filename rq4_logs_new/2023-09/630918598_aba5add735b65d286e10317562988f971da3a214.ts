import { TRPCError } from "@trpc/server";
import { inject, injectable } from "tsyringe";
import { z } from "zod";

import { CreatePlayerUseCase } from "./../../usecase/player/create";
import { publicProcedure, router } from "../trpc";

import {
  playerIdSchema,
  playerNameSchema,
  playerRoleSchema,
  playerStatusSchema,
} from "@/domain/entity/player";
import { roomIdSchema } from "@/domain/entity/room";
import { UseCaseError } from "@/error/usecase/common";
import { RepositoryOperationError } from "@/error/usecase/room";

const playerObjSchema = z.object({
  id: playerIdSchema,
  name: playerNameSchema,
  role: playerRoleSchema,
  status: playerStatusSchema,
  roomId: roomIdSchema.optional(),
});
const createPlayerSchema = z.object({
  name: playerNameSchema,
});

@injectable()
export class PlayerRouter {
  constructor(@inject(CreatePlayerUseCase) private createPlayerUseCase: CreatePlayerUseCase) {}

  public execute() {
    return router({
      create: publicProcedure
        .input(createPlayerSchema)
        .mutation((opts): z.infer<typeof playerObjSchema> => {
          const { input } = opts;

          const createPlayerResult = this.createPlayerUseCase.execute(input.name);
          if (createPlayerResult.err) {
            const errorOpts = ((e: UseCaseError): ConstructorParameters<typeof TRPCError>[0] => {
              if (e instanceof RepositoryOperationError)
                return {
                  message: "Repository operation error",
                  code: "INTERNAL_SERVER_ERROR",
                  cause: e,
                };
              else return { message: "Something was happend", code: "INTERNAL_SERVER_ERROR" };
            })(createPlayerResult.val);
            throw new TRPCError(errorOpts);
          }

          const player = createPlayerResult.unwrap();

          return {
            id: player.id,
            name: player.name,
            role: player.role,
            status: player.status,
            roomId: player.roomId,
          };
        }),
    });
  }
}