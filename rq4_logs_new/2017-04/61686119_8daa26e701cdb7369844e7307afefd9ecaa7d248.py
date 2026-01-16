import spirit_method
bool_types = ["10",
              "yn",
              "tf",
              "of"
             ]
def init(Value, Constraint):
    sem_value = []
    if Constraint != "10":
        sens_bool = spirit_method.sens(Value)
        sem_value.append(sens_bool)
    wrong_bool_type = spirit_method.chosewrong(Constraint, bool_types)
    wrong_bool = spirit_method.bool_generate(wrong_bool_type)
    sem_value.append(wrong_bool)
    return sem_value
