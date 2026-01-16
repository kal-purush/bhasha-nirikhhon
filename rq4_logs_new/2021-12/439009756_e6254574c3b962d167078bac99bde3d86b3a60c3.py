from pgmpy.models import BayesianNetwork
from pgmpy.factors.discrete import TabularCPD
from pgmpy.inference import VariableElimination

model = BayesianNetwork([('D', 'G'), ('I', 'G'), ('G', 'P'), ('I', 'Z')])

cpd_d = TabularCPD(variable='D', variable_card=2, values=[[0.7], [0.3]])
cpd_i = TabularCPD(variable='I', variable_card=2, values=[[0.6], [0.4]])


cpd_g = TabularCPD(variable='G', variable_card=3,
                   values=[[0.2, 0.05, 0.9,  0.6],
                           [0.3, 0.10, 0.08, 0.1],
                           [0.5, 0.85,  0.02, 0.3]],
                  evidence=['I', 'D'],
                  evidence_card=[2, 2])

cpd_p = TabularCPD(variable='P', variable_card=2,
                   values=[[0.1, 0.3, 0.65],
                           [0.9, 0.7, 0.35]],
                   evidence=['G'],
                   evidence_card=[3])

cpd_z = TabularCPD(variable='Z', variable_card=2,
                   values=[[0.85, 0.3],
                           [0.15, 0.7]],
                   evidence=['I'],
                   evidence_card=[2])

model.add_cpds(cpd_d, cpd_i, cpd_g, cpd_p, cpd_z)

model.check_model()


cpd_d_sn = TabularCPD(variable='D', variable_card=2, values=[[0.6], [0.4]], state_names={'D': ['Легкий', 'Складний']})
cpd_i_sn = TabularCPD(variable='I', variable_card=2, values=[[0.7], [0.3]], state_names={'I': ['Ниж.сер.', 'Вищ.сер.']})
cpd_g_sn = TabularCPD(variable='G', variable_card=3,
                      values=[[0.2, 0.05, 0.9,  0.6],
                           [0.3, 0.10, 0.08, 0.1],
                           [0.5, 0.85,  0.02, 0.3]],
                      evidence=['I', 'D'],
                      evidence_card=[2, 2],
                      state_names={'G': ['Хороша', 'Задовільно', 'Незадовільно'],
                                   'I': ['Ниж.сер.', 'Вищ.сер.'],
                                   'D': ['Легкий', 'Складний']})

cpd_p_sn = TabularCPD(variable='P', variable_card=2,
                      values=[[0.1, 0.3, 0.65],
                           [0.9, 0.7, 0.35]],
                      evidence=['G'],
                      evidence_card=[3],
                      state_names={'P': ['Не проблемний', 'Проблемний'],
                                   'G': ['Хороша', 'Задовільно', 'Незадовільно']})

cpd_z_sn = TabularCPD(variable='Z', variable_card=2,
                      values=[[0.85, 0.3],
                           [0.15, 0.7]],
                      evidence=['I'],
                      evidence_card=[2],
                      state_names={'Z': ['Пог. бал', 'Хор. бал'],
                                   'I': ['Ниж.сер.', 'Вищ.сер.']})

model.add_cpds(cpd_d_sn, cpd_i_sn, cpd_g_sn, cpd_p_sn, cpd_z_sn)
model.check_model()

model.get_cpds()

print(cpd_g)
print(model.get_cpds('G'))


infer = VariableElimination(model)
g_dist = infer.query(['G'])
print(g_dist)

print(infer.query(['G'], evidence={'D': 'Легкий', 'I': 'Вищ.сер.'}))
print(infer.query(['G'], evidence={'D': 'Легкий', 'I': 'Ниж.сер.', 'P': 'Не проблемний', 'Z': 'Пог. бал'}))