import { ApplicationConfig, provideZonelessChangeDetection } from '@angular/core';

// Zoneless change detection (signals-driven)
export const appConfig: ApplicationConfig = {
  providers: [provideZonelessChangeDetection()]
};
