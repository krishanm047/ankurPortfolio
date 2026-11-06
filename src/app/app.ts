import { Component, signal } from '@angular/core';
import { TitleCasePipe } from '@angular/common';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatCardModule } from '@angular/material/card';
import { MatChipsModule } from '@angular/material/chips';

interface Item { icon: string; title: string; text: string; }
interface Milestone { period: string; role: string; org: string; points: string[]; }

@Component({
  selector: 'app-root',
  imports: [TitleCasePipe, MatToolbarModule, MatButtonModule, MatIconModule,
            MatCardModule, MatChipsModule],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App {
  // Mobile menu toggle state
  menuOpen = signal(false);
  year = new Date().getFullYear();

  // Hero photo served from public/ankur.png
  photo = 'ankur.png';
  photoOk = signal(true);

  nav = ['about', 'experience', 'journey', 'passion', 'skills', 'contact'];

  // Contact details (from LinkedIn profile)
  email = 'ankuroff619@gmail.com';
  linkedin = 'https://www.linkedin.com/in/ankur-singh-320079172/';
  location = 'Kolkata, West Bengal, India';

  // Content below is based on Ankur's LinkedIn profile
  bio = `Automobile enthusiast and MBA (Marketing & HR) from KIIT School of Management, based in
    Kolkata. Ankur is Deputy Manager at Hero MotoCorp, driving sales for the Harley-Davidson unit
    across West Bengal and Odisha. Before that, he spent over two and a half years at TVS Motor
    Company, growing from Management Trainee to Territory Manager across Bengaluru, Amritsar and
    Bhubaneswar.`;

  quote = 'Chase experiences.';

  stats = [
    { value: '4+ Years', label: 'Two-Wheeler Industry' },
    { value: 'Harley-Davidson', label: 'Hero MotoCorp' },
    { value: 'WB & Odisha', label: 'Current Region' },
    { value: '4 Cities', label: 'Worked Across' }
  ];

  experience: Item[] = [
    { icon: 'two_wheeler', title: 'Harley-Davidson Sales',
      text: 'Driving sales for the Harley-Davidson unit of Hero MotoCorp in West Bengal and Odisha.' },
    { icon: 'map', title: 'Territory Management',
      text: 'Territory Manager roles at TVS Motor Company in Amritsar and Bhubaneswar.' },
    { icon: 'insights', title: 'Analysis-Led Decisions',
      text: 'Brings analytical, data and business analysis skills to sales and territory planning.' }
  ];

  journey: Milestone[] = [
    { period: 'Nov 2024 – Present', role: 'Deputy Manager', org: 'Hero MotoCorp · Kolkata',
      points: ['Driving sales for the Harley-Davidson unit, West Bengal and Odisha region'] },
    { period: 'Jul 2024 – Nov 2024', role: 'Territory Manager', org: 'TVS Motor Company · Bhubaneswar', points: [] },
    { period: 'Jan 2024 – Jul 2024', role: 'Territory Manager', org: 'TVS Motor Company · Amritsar', points: [] },
    { period: 'May 2023 – Jan 2024', role: 'Assistant Manager', org: 'TVS Motor Company · Bengaluru', points: [] },
    { period: 'May 2022 – Jun 2023', role: 'Management Trainee', org: 'TVS Motor Company · Bengaluru', points: [] }
  ];

  education: Milestone[] = [
    { period: '2020 – 2022', role: 'MBA, Marketing & HR', org: 'KIIT School of Management (KSOM), Bhubaneswar', points: [] },
    { period: '2015 – 2019', role: 'Undergraduate Studies', org: 'Trident Academy of Technology (TAT), Bhubaneswar', points: [] },
    { period: '2013', role: 'High School Diploma', org: 'Nazareth Academy', points: [] }
  ];

  passions: Item[] = [
    { icon: 'two_wheeler', title: 'Motorcycles', text: 'Working with one of the world\'s most iconic motorcycle brands.' },
    { icon: 'directions_car', title: 'Automobiles', text: 'A self-described automobile enthusiast.' },
    { icon: 'explore', title: 'Chasing Experiences', text: 'His motto: always chase the next experience.' }
  ];

  // Top skills as listed on LinkedIn
  skills: Item[] = [
    { icon: 'analytics', title: 'Analytical Skills', text: '' },
    { icon: 'bar_chart', title: 'Data Analysis', text: '' },
    { icon: 'business_center', title: 'Business Analysis', text: '' }
  ];

  tags = ['Sales', 'Territory Management', 'Two-Wheeler Industry', 'Marketing', 'HR'];

  // Smooth-scroll to a section and close the mobile menu
  scrollTo(id: string): void {
    document.getElementById(id)?.scrollIntoView({ behavior: 'smooth' });
    this.menuOpen.set(false);
  }
}
