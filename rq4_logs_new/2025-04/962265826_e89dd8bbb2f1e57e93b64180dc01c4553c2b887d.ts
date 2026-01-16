import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { SidebarComponent } from '../dashboard/sidebar/sidebar.component';


interface Choice {
  [key: string]: string;  // Allows dynamic keys like 'a', 'b', 'c'
}

type ChoiceKey = 'a' | 'b' | 'c';

interface Question {
  question: string;
  choices: Choice;
  right: string[];
}

interface Quiz {
  id: number;
  title: string;
  date: string;
  questions: Question[];
}

@Component({
  selector: 'app-quiz-management',
  templateUrl: './quiz-management.component.html',
  imports:[CommonModule, FormsModule, SidebarComponent],
  styleUrls: ['./quiz-management.component.css'],
})
export class QuizManagementComponent implements OnInit {
  subjectId!: string;
  quizzes: Quiz[] = [];
  showAddForm = false;

  filter: string = 'date';
  sortOrder: string = 'asc';

  newQuiz: Quiz = {
    id: 0,
    title: '',
    date: '',
    questions: [
      {
        question: '',
        choices: { a: '', b: '', c: '' },
        right: [],
      },
    ],
  };

  currentQuestionIndex: number = 0;

  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.subjectId = this.route.snapshot.paramMap.get('id') || 'unknown';
    this.loadQuizzes();
  }

  // Toggle add form visibility
  toggleAddForm() {
    this.showAddForm = !this.showAddForm;
  }

  // Add a new question to the quiz
  addQuestion() {
    this.newQuiz.questions.push({
      question: '',
      choices: { a: '', b: '', c: '' },
      right: [],
    });
    this.currentQuestionIndex = this.newQuiz.questions.length - 1;
  }

  // Remove the current question
  removeQuestion() {
    if (this.newQuiz.questions.length > 1) {
      this.newQuiz.questions.splice(this.currentQuestionIndex, 1);
      this.currentQuestionIndex = Math.max(0, this.currentQuestionIndex - 1);
    }
  }

  // Navigate to the next question
  nextQuestion() {
    if (this.currentQuestionIndex < this.newQuiz.questions.length - 1) {
      this.currentQuestionIndex++;
    }
  }

  // Navigate to the previous question
  previousQuestion() {
    if (this.currentQuestionIndex > 0) {
      this.currentQuestionIndex--;
    }
  }

  // Add a new quiz to the list
  addQuiz() {
    const newQuizCopy = JSON.parse(JSON.stringify(this.newQuiz));
    newQuizCopy.id = Date.now();
    this.quizzes.push(newQuizCopy);
    this.resetForm();
    this.showAddForm = false;
  }

  // Delete a quiz by its ID
  deleteQuiz(id: number) {
    this.quizzes = this.quizzes.filter((q) => q.id !== id);
  }

  // Reset the add quiz form
  resetForm() {
    this.newQuiz = {
      id: 0,
      title: '',
      date: '',
      questions: [
        {
          question: '',
          choices: { a: '', b: '', c: '' },
          right: [],
        },
      ],
    };
    this.currentQuestionIndex = 0;
  }

  // Sort quizzes based on filter and sort order
  sortQuizzes() {
    this.quizzes.sort((a, b) => {
      let compareVal = 0;

      if (this.filter === 'date') {
        compareVal = new Date(a.date).getTime() - new Date(b.date).getTime();
      } else if (this.filter === 'alphabetic') {
        compareVal = a.title.localeCompare(b.title);
      }

      return this.sortOrder === 'asc' ? compareVal : -compareVal;
    });
  }

  // Track the index of questions
  trackByIndex(index: number): number {
    return index;
  }

  // Get the current question
  get currentQuestion(): Question {
    return this.newQuiz.questions[this.currentQuestionIndex];
  }

  // Update the value of a choice
  updateChoiceValue(choiceKey: ChoiceKey, value: string) {
    const choice = this.currentQuestion.choices[choiceKey];
    if (choice !== undefined) {
      this.currentQuestion.choices[choiceKey] = value;
    }
  }

  // Load quizzes (hardcoded for now or you could fetch from a service)
  loadQuizzes() {
    this.quizzes = [
      {
        id: 1,
        title: 'Sample Quiz 1',
        date: '2025-05-01',
        questions: [
          {
            question: 'What is 2 + 2?',
            choices: {
              a: '3',
              b: '4',
              c: '5',
            },
            right: ['b'],
          },
        ],
      },
    ];
  }
  isSidebarCollapsed = false;

  onSidebarToggle() {
    this.isSidebarCollapsed = !this.isSidebarCollapsed;
  }
}