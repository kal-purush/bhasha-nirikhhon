using System;
using System.Collections;
using System.Collections.Generic;

namespace BatalhaNaval
{
    /// <summary>
    /// Lista para tiros e resultados
    /// </summary>
    public class ListaDeTiros : IEnumerable<Tiro>
    {
        /// <summary>
        /// Tiros da lista
        /// </summary>
        private Dictionary<Tiro, ResultadoDeTiro> _tiros;

        /// <summary>
        /// Construtor
        /// </summary>
        internal ListaDeTiros()
        {
            _tiros = new Dictionary<Tiro, ResultadoDeTiro>();
        }

        /// <summary>
        /// Adiciona um elemento na lista
        /// </summary>
        /// <param name="t">Tiro</param>
        /// <param name="r">Resultado do tiro</param>
        internal void Add(Tiro t, ResultadoDeTiro r)
        {
            _tiros.Add(t, r);
        }

        /// <summary>
        /// Obtém o resultado de um tiro da lista
        /// </summary>
        /// <param name="t">Tiro presente na lista</param>
        /// <returns>O resultado do tiro ou ResultadoDeTiro.Invalido caso o tiro não exista na lista</returns>
        public ResultadoDeTiro Resultado(Tiro t)
        {
            if (_tiros.ContainsKey(t))
                return _tiros[t];

            return ResultadoDeTiro.Invalido;
        }

        /// <summary>
        /// Obtém um enumerador de tipo específico da lista
        /// </summary>
        /// <returns>Um enumerador que percorre todos os tiros da lista</returns>
        public IEnumerator<Tiro> GetEnumerator()
        {
            return _tiros.Keys.GetEnumerator();
        }
        
        /// <summary>
        /// Obtém um enumerador genérico da lista
        /// </summary>
        /// <returns>Um enumerador que percorre todos os tiros da lista</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}