import React, {useEffect, useState} from 'react';
import {makeStyles} from '@material-ui/core/styles';
import {Dialog, DialogActions, DialogContent, DialogTitle, Grid, TextField} from "@material-ui/core";
import Button from "@material-ui/core/Button";
import CheckIcon from '@material-ui/icons/Check';
import CloseIcon from '@material-ui/icons/Close';
import isInn from "is-inn-js";
import Typography from "@material-ui/core/Typography";

const useStyles = makeStyles(() => ({
    modalTitle: {
        fontSize: 18
    },
    supplyTitle: {
        marginBottom: "15px",
        textAlign: "center"
    },
    supplyInput: {
        marginBottom: "15px"
    },
}));

const AddOrganizationModal = ({isOpenModal, closeModal, subjectId, submitModalForm}) => {
    const initialFormState = {
        naim_org: '',
        npp: '',
        r1022: subjectId,
        adr_fact: '',
        inn: '',
        plazma_max: '',
        plazma_cena: '',
        erm_max: '',
        erm_cena: '',
        immg_max: '',
        immg_cena: '',
        alb_max: '',
        alb_cena: '',
    };

    const innErrorInitialState = {
        isInnError: true,
        innErrorMessage: 'Введите ИНН'
    };

    const isDataValidInitialState = {
        naim_org_valid: false,
        npp_valid: false,
        adr_fact_valid: false,
        inn_valid: false,
        plazma_max_valid: false,
        plazma_cena_valid: false,
        erm_max_valid: false,
        erm_cena_valid: false,
        immg_max_valid: false,
        immg_cena_valid: false,
        alb_max_valid: false,
        alb_cena_valid: false,
    }

    const [formData, setFormData] = useState(initialFormState);
    const [innError, setInnError] = useState(innErrorInitialState);
    const [isDataValid, setIsDataValid] = useState(isDataValidInitialState);
    const [isSubmitDisabled, setIsSubmitDisabled] = useState(true);

    const classes = useStyles();

    useEffect(() => {
        setFormData((formData) => ({...formData, r1022: subjectId}))
    }, [subjectId]);

    const checkFormValidation = () => {
        setIsSubmitDisabled(!Object.values(isDataValid).every((item) => {
            return item === true;
        }))
    }

    const checkMaxAndPriceValidation = (key) => {
        return (formData[key] === '' || !!formData[key].match(/^\d+(?:[.,]\d{1,6})?$/))
    }

    useEffect(() => {
        setIsDataValid({
            naim_org_valid: formData.naim_org !== '',
            npp_valid: Number.isInteger(+formData.npp),
            adr_fact_valid: formData.adr_fact !== '',
            inn_valid: isInn(formData.inn),
            plazma_max_valid: checkMaxAndPriceValidation('plazma_max'),
            plazma_cena_valid: checkMaxAndPriceValidation('plazma_cena'),
            erm_max_valid: checkMaxAndPriceValidation('erm_max'),
            erm_cena_valid: checkMaxAndPriceValidation('erm_cena'),
            immg_max_valid: checkMaxAndPriceValidation('immg_max'),
            immg_cena_valid: checkMaxAndPriceValidation('immg_cena'),
            alb_max_valid: checkMaxAndPriceValidation('alb_max'),
            alb_cena_valid: checkMaxAndPriceValidation('alb_cena'),
        })
        // eslint-disable-next-line
    }, [formData])

    useEffect(() => {
        checkFormValidation();
        // eslint-disable-next-line
    }, [isDataValid])

    const handleClose = () => {
        closeModal();
        setFormData(initialFormState);
        setInnError(innErrorInitialState);
        setIsDataValid(isDataValidInitialState);
    };

    const handleDataChange = (target) => {
        setFormData(prevState => ({
            ...prevState,
            [target.name]: target.value
        }))
        if (target.name === 'inn') {
            if (target.value !== '') {
                setInnError({
                    innErrorMessage: isInn(target.value) ? '' : 'Некорректный ИНН',
                    isInnError: !isInn(target.value),
                })
            } else {
                setInnError({
                    innErrorMessage: 'Введите ИНН',
                    isInnError: !isInn(target.value),
                })
            }
        }
    }

    return (
        <div>
            <Dialog fullWidth={true} maxWidth="md" onClose={handleClose} disableBackdropClick={true}
                    open={isOpenModal}>
                <DialogTitle onClose={handleClose} className={classes.modalTitle}>
                    Добавление организации
                </DialogTitle>
                <DialogContent dividers>
                    <form id="org-data-form">
                        <Grid container spacing={2}>
                            <Grid item sm={6}>
                                <TextField
                                    fullWidth
                                    required
                                    error={!formData.naim_org}
                                    label="Наименование организации"
                                    helperText={formData.naim_org ? '' : 'Введите наименование организации'}
                                    name="naim_org"
                                    value={formData.naim_org}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                            <Grid item sm={6}>
                                <TextField
                                    fullWidth
                                    required
                                    error={!formData.adr_fact}
                                    label="Фактический адрес"
                                    helperText={formData.adr_fact ? '' : 'Введите адрес организации'}
                                    name="adr_fact"
                                    value={formData.adr_fact}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                            <Grid item sm={4}>
                                <TextField
                                    fullWidth
                                    required
                                    error={!formData.npp || !Number.isInteger(+formData.npp)}
                                    label="Номер п.п."
                                    helperText={
                                        !formData.npp
                                            ? 'Введите номер п.п.'
                                            : (Number.isInteger(+formData.npp)
                                            ? ''
                                            : 'Некорректный номер п.п.')
                                    }
                                    name="npp"
                                    value={formData.npp}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                            <Grid item sm={4}>
                                <TextField
                                    fullWidth
                                    required
                                    error={innError.isInnError}
                                    label="ИНН"
                                    helperText={innError.innErrorMessage}
                                    name="inn"
                                    value={formData.inn}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                            <Grid item sm={4}>
                                <TextField
                                    fullWidth
                                    label="Номер субъекта РФ"
                                    name="r1022"
                                    value={formData.r1022}
                                    variant="outlined"
                                    disabled
                                />
                            </Grid>
                            <Grid item sm={12}>
                                <Typography variant="h6">Возможности поставок организации</Typography>
                            </Grid>
                            <Grid item sm={3}>
                                <Typography variant="body1" className={classes.supplyTitle}>Плазма</Typography>
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Макс. объем (тыс. литров)"
                                    error={!isDataValid.plazma_max_valid}
                                    helperText={!isDataValid.plazma_max_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="plazma_max"
                                    value={formData.plazma_max}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Цена (тыс. руб. за 1 литр)"
                                    error={!isDataValid.plazma_cena_valid}
                                    helperText={!isDataValid.plazma_cena_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="plazma_cena"
                                    value={formData.plazma_cena}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                            <Grid item sm={3}>
                                <Typography variant="body1" className={classes.supplyTitle}>Эритроцитарная
                                    масса</Typography>
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Макс. объем (тыс. литров)"
                                    error={!isDataValid.erm_max_valid}
                                    helperText={!isDataValid.erm_max_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="erm_max"
                                    value={formData.erm_max}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Цена (тыс. руб. за 1 литр)"
                                    error={!isDataValid.erm_cena_valid}
                                    helperText={!isDataValid.erm_cena_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="erm_cena"
                                    value={formData.erm_cena}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                            <Grid item sm={3}>
                                <Typography variant="body1" className={classes.supplyTitle}>Иммуноглобулин
                                    человека</Typography>
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Макс. объем (тыс. литров)"
                                    error={!isDataValid.immg_max_valid}
                                    helperText={!isDataValid.immg_max_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="immg_max"
                                    value={formData.immg_max}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Цена (тыс. руб. за 1 литр)"
                                    error={!isDataValid.immg_cena_valid}
                                    helperText={!isDataValid.immg_cena_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="immg_cena"
                                    value={formData.immg_cena}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                            <Grid item sm={3}>
                                <Typography variant="body1" className={classes.supplyTitle}>Альбумин
                                    10-процентный</Typography>
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Макс. объем (тыс. литров)"
                                    error={!isDataValid.alb_max_valid}
                                    helperText={!isDataValid.alb_max_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="alb_max"
                                    value={formData.alb_max}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                                <TextField
                                    className={classes.supplyInput}
                                    fullWidth
                                    label="Цена (тыс. руб. за 1 литр)"
                                    error={!isDataValid.alb_cena_valid}
                                    helperText={!isDataValid.alb_cena_valid
                                        ? 'Некорректное число'
                                        : ''
                                    }
                                    name="alb_cena"
                                    value={formData.alb_cena}
                                    variant="outlined"
                                    onChange={(e) => handleDataChange(e.target)}
                                />
                            </Grid>
                        </Grid>
                    </form>
                </DialogContent>
                <DialogActions>
                    <Button variant="contained" color="primary" type="submit" form="org-data-form" onClick={(e) => {
                        e.preventDefault();
                        submitModalForm(formData);
                    }}
                            disabled={isSubmitDisabled}
                            endIcon={<CheckIcon/>}>
                        Сохранить
                    </Button>
                    <Button variant="contained" color="secondary" onClick={handleClose} endIcon={<CloseIcon/>}>
                        Закрыть
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default AddOrganizationModal;