export interface Note {
  id: string;
  title: string;
  content: string;
  date: string;
}

export interface Material {
  id: string;
  title: string;
  type: 'pdf' | 'ppt' | 'doc';
  size: string;
  uploadDate: string;
  downloadUrl: string;
}

export const notes: Note[] = [
  {
    id: '1',
    title: 'Project Phase 1 Guidelines',
    content: 'Important points for the first phase of the project: 1. Focus on requirement gathering 2. Create initial design documents 3. Prepare project timeline',
    date: '2024-03-25'
  },
  {
    id: '2',
    title: 'Design Patterns Overview',
    content: 'We will cover the following design patterns in next class: Factory, Singleton, Observer, and Strategy patterns.',
    date: '2024-03-24'
  },
  {
    id: '3',
    title: 'Design Pawdwdwdtterns Overview',
    content: 'We will cover the following design patterns in next class: Factory, Singleton, Observer, and Strategy patterns.',
    date: '2024-03-24'
  }
];

export const materials: Material[] = [
  {
    id: '1',
    title: 'Week 1 - Introduction to Software Design',
    type: 'pdf',
    size: '2.5 MB',
    uploadDate: '2024-03-20',
    downloadUrl: '/materials/week1.pdf'
  },
  {
    id: '2',
    title: 'Week 2 - Design Principles',
    type: 'ppt',
    size: '4.8 MB',
    uploadDate: '2024-03-22',
    downloadUrl: '/materials/week2.pptx'
  },
  {
    id: '3',
    title: 'Project Requirements Document',
    type: 'doc',
    size: '1.2 MB',
    uploadDate: '2024-03-24',
    downloadUrl: '/materials/requirements.docx'
  }
];