#note: this is another version/combination of accuracies. despite having these accuracies, the model performed fine on hpc. So am not sure which one of the accuracies is right to test on laptop . so doing both.
[BERT]
model_name_or_path="bert-base-cased"
task_name="fevercrossdomain"
do_train=True
do_eval=True
do_predict=True
do_train_1student_1teacher=True
data_dir="/Users/mordor/research/huggingface/src/transformers/data/datasets/fever/fevercrossdomain/combined/figerspecific/toydata/"
max_seq_length="128"
per_device_eval_batch_size="16"
per_device_train_batch_size="16"
learning_rate="1e-5"
# Note: use num_train_epochs=1 only. For testing purposes, in trainery.py we are as of now, sep 2020 we are returning the dev and test partition evaluation results for epoch1
# if you want to test for more than 1 eopch, return best_dev and best_test values
num_train_epochs="1"
output_dir="/Users/mordor/research/huggingface/mithun_scripts/output/fever/fevercrossdomain/combined/figerspecific/bert-base-cased/128/"
overwrite_output_dir=True
weight_decay="0.01"
adam_epsilon="1e-6"
evaluate_during_training=True
task_type="combined"
subtask_type="figerspecific"
machine_to_run_on="laptop"
toy_data_dir_path="/Users/mordor/research/huggingface/src/transformers/data/datasets/fever/fevercrossdomain/combined/figerspecific/toydata/"
#what are the scores that you should assert against. i.e the ones we got when we ran the toy data alone
fever_in_domain_accuracy_on_toy_data_17_datapoints=0.0
fever_cross_domain_accuracy_on_toy_data_17_datapoints=0.0625
fever_cross_domain_fncscore_on_toy_data_17_datapoints=0.275