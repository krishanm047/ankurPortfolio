import { bootstrapApplication } from '@angular/platform-browser';
import { App } from './app/app';
import { appConfig } from './app/app.config';

// Bootstrap the standalone root component
bootstrapApplication(App, appConfig).catch(err => console.error(err));
