namespace MentalHospital.BLL.Services
{
    public class PatientService : GenericService<PatientModel, Patient>, IGenericService<PatientModel>
    {
        private readonly IPatientRepository _repository;
        private readonly IMapper _mapper;

        public PatientService(IPatientRepository repository, IMapper mapper) : base (repository, mapper)
        {
            _repository = repository;
            _mapper = mapper;
        }

        public async Task<PatientModel> Create(PatientModel model)
        {
            var entity = await _repository.Create(_mapper.Map<Patient>(model));
            return _mapper.Map<PatientModel>(entity);
        }

        public async Task<PatientModel> Update(PatientModel model)
        {
            var entity = await _repository.Update(_mapper.Map<Patient>(model));
            return _mapper.Map<PatientModel>(entity);
        }
    }
}