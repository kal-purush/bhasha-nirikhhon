def preprocessing(file_path, field_length, write_path, sep=','):
    import re
    f = open(file_path)
    arq = f.readlines()
    f.close()
    if field_length != len(arq[0].split(sep)):
        raise ValueError('O header tem tamanho diferente do informado `expected {} \\ {}'.format(len(arq[0].split(sep)), field_length))
    quote_ctrl = '_quote_control_'
    field_separator = '_field_separator_'
    aux_start_sep = '_auxiliary_start_separator_'
    aux_end_sep = '_auxiliary_end_separator_'

    j = 0
    new_arq = [arq[0]]
    for i, line in enumerate(arq):
        if i <= j: continue
        new_line = line
        j = i + 1
        line_aux = new_line + arq[j]
        while((len(line_aux.split(sep)) <= field_length) and (j < len(arq))):
            j += 1
            new_line = line_aux
            if j < len(arq): line_aux += arq[j]
        if len(line_aux.split(sep)) > field_length:
            j -= 1
            # raise ValueError('Linha com algum separador dentro dos campos!')
        # clean lines
        new_line = re.sub('[\\\\]*\\\"' + sep + '\\\"', field_separator, new_line)
        new_line = re.sub('[\\\\]*\\\"' + sep, aux_end_sep, new_line)
        new_line = re.sub('[\\\\]*' + sep + '\\\"', aux_start_sep, new_line)
        new_line = re.sub('^\\\"|\\\"$', quote_ctrl, new_line)
        new_line = re.sub('[\\\\]*\\\"', ' ', new_line)
        new_line = re.sub(field_separator, '","', new_line)
        new_line = re.sub(aux_start_sep, ',"', new_line)
        new_line = re.sub(aux_end_sep, '",', new_line)
        new_line = re.sub(quote_ctrl, '"', new_line)

        new_line = re.sub('[\n\t\r ]+', ' ', new_line).strip() + '\n'
        if new_line != '\n': new_arq.append(new_line)
    print(f'Writing {len(new_arq)} lines...')
    f = open(write_path, 'w')
    for line in new_arq:
        f.write(line)
    f.close()