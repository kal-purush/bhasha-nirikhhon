import Assignment from '../models/assignamentModel.js';
import Employee from '../models/employeeModel.js';
import Project from '../models/projectModel.js';

const assignEmployeeToProject = async (req, res) => {
    try {
        const { employee_id, project_id, start_date, end_date } = req.body;
        if (!employee_id || !project_id || !start_date || !end_date) {
            return res.status(400).json({ error: 'Employee ID, Project ID, start date, and end date are required' });
        }
        const project = await Project.findByPk(project_id);
        if (!project) {
            return res.status(404).json({ error: 'Project not found' });
        }

        const employee = await Employee.findByPk(employee_id);
        if (!employee) {
            return res.status(404).json({ error: 'Employee not found' });
        }

        const existingAssignment = await Assignment.findOne({
            where: { employee_id: employee_id }
        });
        if (existingAssignment) {
            return res.status(400).json({ error: 'Employee is already assigned to a project' });
        }
        const assignment = await Assignment.create({
            employee_id: employee_id,
            project_id: project_id,
            start_date: start_date,
            end_date: end_date
        });

        res.status(201).json(assignment);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Error assigning employee to project' });
    }
};

export { assignEmployeeToProject };