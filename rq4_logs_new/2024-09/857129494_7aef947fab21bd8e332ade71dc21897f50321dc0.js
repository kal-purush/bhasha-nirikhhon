import React, { useState, useEffect } from 'react';
import api from '../utils/api';
import { motion, AnimatePresence } from 'framer-motion';
import { Database, ChevronDown, ChevronUp, Loader2, AlertTriangle } from 'lucide-react';

const styles = {
  container: {
    minHeight: '100vh',
    background: 'linear-gradient(135deg, #033225, #0A4F4C, #15296E)',
    color: 'white',
    padding: '4rem 1rem',
    position: 'relative',
    overflow: 'hidden',
  },
  content: {
    maxWidth: '80rem',
    margin: '0 auto',
  },
  title: {
    fontSize: 'clamp(2.5rem, 5vw, 4.5rem)',
    fontWeight: '800',
    marginBottom: '2rem',
    textAlign: 'center',
    color: '#FFFFFF',
    textShadow: '2px 2px 4px rgba(0, 0, 0, 0.3)',
  },
  subtitle: {
    fontSize: '1.75rem',
    fontWeight: '600',
    marginBottom: '1rem',
    textAlign: 'center',
    color: '#F3F4F6',
  },
  sectionButton: {
    width: '100%',
    padding: '1rem 1.5rem',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    textAlign: 'left',
    fontSize: '1.25rem',
    fontWeight: '600',
    color: '#10B981',
    backgroundColor: 'rgba(255, 255, 255, 0.1)',
    border: 'none',
    borderRadius: '0.5rem',
    cursor: 'pointer',
    transition: 'all 0.3s',
  },
  table: {
    width: '100%',
    borderCollapse: 'separate',
    borderSpacing: '0 0.5rem',
  },
  th: {
    padding: '1rem',
    textAlign: 'left',
    color: '#10B981',
    fontWeight: '600',
  },
  td: {
    padding: '1rem',
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
    color: '#F3F4F6',
  },
  error: {
    backgroundColor: 'rgba(220, 38, 38, 0.2)',
    color: '#FCA5A5',
    padding: '1rem',
    borderRadius: '0.5rem',
    marginBottom: '2rem',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
};
const ErrorDisplay = ({ error }) => (
  <motion.div 
    style={{
      ...styles.error,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      padding: '0.75rem 1.5rem',
      backgroundColor: 'rgba(220, 38, 38, 0.2)',
      color: '#FCA5A5',
      borderRadius: '0.5rem',
      marginBottom: '2rem',
      marginTop: '2rem',
      maxWidth: '700px',
      margin: '2rem auto',
      fontSize: '1rem',
      fontWeight: '600',
      boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
    }}
    initial={{ opacity: 0, y: -20 }}
    animate={{ opacity: 1, y: 0 }}
    exit={{ opacity: 0, y: -20 }}
    transition={{ duration: 0.5 }}
  >
    <AlertTriangle style={{ marginRight: '0.75rem' }} size={24} />
    <span>{error}</span>
  </motion.div>
);

function SampleData() {
  const [sampleData, setSampleData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedSection, setExpandedSection] = useState(null);
  const [scrollY, setScrollY] = useState(0);

  useEffect(() => {
    const fetchSampleData = async () => {
      try {
        const response = await api.get('/api/sample-data/');
        setSampleData(response.data);
        setLoading(false);
      } catch (err) {
        console.error('Error fetching sample data:', err);
        setError('Failed to fetch sample data. Please try again later.');
        setLoading(false);
      }
    };

    fetchSampleData();
  }, []);

  useEffect(() => {
    const handleScroll = () => setScrollY(window.scrollY);
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const toggleSection = (section) => {
    setExpandedSection(expandedSection === section ? null : section);
  };

  if (loading) {
    return (
      <div style={styles.container}>
        <div style={styles.content}>
          <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
            <Loader2 size={48} className="animate-spin text-cyan-400" />
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={styles.container}>
      <div style={styles.content}>
        <motion.h1
          style={styles.title}
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
        >
          Sample Data
        </motion.h1>

        <motion.h2
          style={styles.subtitle}
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.2 }}
        >
          Snippets of few entries from our 3 datasets (200+ entries..)
        </motion.h2>

        <AnimatePresence>
          {error && <ErrorDisplay error={error} />}
        </AnimatePresence>

        {sampleData && Object.entries(sampleData).map(([key, data], index) => (
          <motion.div
            key={key}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: index * 0.1 }}
            style={{ marginBottom: '1.5rem' }}
          >
            <motion.button
              style={styles.sectionButton}
              onClick={() => toggleSection(key)}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              <span style={{ display: 'flex', alignItems: 'center' }}>
                <Database style={{ marginRight: '0.5rem' }} size={24} />
                {key.replace(/_/g, ' ').replace('sample', '').trim()}
              </span>
              {expandedSection === key ? <ChevronUp size={24} /> : <ChevronDown size={24} />}
            </motion.button>
            <AnimatePresence>
              {expandedSection === key && (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  exit={{ opacity: 0, height: 0 }}
                  transition={{ duration: 0.3 }}
                  style={{ overflow: 'hidden' }}
                >
                  <div style={{ padding: '1rem', overflowX: 'auto' }}>
                    <table style={styles.table}>
                      <thead>
                        <tr>
                          {Object.keys(data[0]).map((header) => (
                            <th key={header} style={styles.th}>{header}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {data.map((row, index) => (
                          <tr key={index}>
                            {Object.values(row).map((value, cellIndex) => (
                              <td key={cellIndex} style={styles.td}>{value}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        ))}
      </div>
      <div
        style={{
          position: 'fixed',
          inset: 0,
          backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='100' height='100' viewBox='0 0 100 100'%3E%3Cg fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.08'%3E%3Cpath opacity='.5' d='M96 95h4v1h-4v4h-1v-4h-9v4h-1v-4h-9v4h-1v-4h-9v4h-1v-4h-9v4h-1v-4h-9v4h-1v-4h-9v4h-1v-4h-9v4h-1v-4h-9v4h-1v-4H0v-1h15v-9H0v-1h15v-9H0v-1h15v-9H0v-1h15v-9H0v-1h15v-9H0v-1h15v-9H0v-1h15v-9H0v-1h15v-9H0v-1h15V0h1v15h9V0h1v15h9V0h1v15h9V0h1v15h9V0h1v15h9V0h1v15h9V0h1v15h9V0h1v15h9V0h1v15h4v1h-4v9h4v1h-4v9h4v1h-4v9h4v1h-4v9h4v1h-4v9h4v1h-4v9h4v1h-4v9h4v1h-4v9zm-1 0v-9h-9v9h9zm-10 0v-9h-9v9h9zm-10 0v-9h-9v9h9zm-10 0v-9h-9v9h9zm-10 0v-9h-9v9h9zm-10 0v-9h-9v9h9zm-10 0v-9h-9v9h9zm-10 0v-9h-9v9h9zm-9-10h9v-9h-9v9h9zm10 0h9v-9h-9v9zm10 0h9v-9h-9v9zm10 0h9v-9h-9v9zm10 0h9v-9h-9v9zm10 0h9v-9h-9v9zm10 0h9v-9h-9v9zm10 0h9v-9h-9v9z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")`,
          transform: `translateY(${scrollY * 0.5}px)`,
          pointerEvents: 'none',
        }}
      />
    </div>
  );
}

export default SampleData;