import { Request, Response } from "express";
import { Project } from "../../models/Project";
import * as fs from "fs";

export const deleteFiles = async (req: Request, res: Response) => {
    const projectId: number = parseInt(req.params.projectId);
    const { path } = req.body;

    if (!path || !projectId)
        return res.status(400).json({ message: "Project Id and Path is required!", error: true });

    try {
        let project = await Project.findByPk(projectId);
        if (!project) return res.status(404).json({ message: "Project not found!", error: true });

        fs.unlink(`public/${path}`, async (err) => {
            if (err)
                return res.status(500).json({
                    message: "An error has occured while deleting file",
                    error: true,
                });

            await Project.update(
                { files: project.dataValues.files.filter((file: string) => file !== path) },
                { where: { id: projectId } }
            );

            return res.status(200).json({ message: "File Deleted", error: false });
        });
    } catch (e: any) {
        console.log("Error while deleting file", e);

        return res.status(500).json({
            message: "An error has occured while deleting file",
            error: true,
        });
    }
};