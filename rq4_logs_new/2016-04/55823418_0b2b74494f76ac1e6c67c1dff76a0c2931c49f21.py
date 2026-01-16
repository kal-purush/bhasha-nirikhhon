import json

def make_control_file_Arry():
	cf = open('control.json' , 'w')	
	metadata = {}
	metadata['location'] = ['data/WineQuality/winequality.data']
	metadata['class_name'] = 'Class'
	metadata['sep'] = ';'
	metadata['attr_names'] = ['N/A'] * 11
	metadata['attr_types'] = ['c'] * 11
	metadata['class_position'] = 11
	
	cf.write(json.dumps(metadata, indent=1))

make_control_file_Arry()