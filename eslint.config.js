import js from '@eslint/js';
import tseslint from '@typescript-eslint/eslint-plugin';
import tsParser from '@typescript-eslint/parser';

export default [
  {
    ignores: [
      '**/dist/**',
      '**/src/**/*.js',
      '**/src/**/*.d.ts',
      '**/*.d.ts',
      'node_modules/**'
    ]
  },
  js.configs.recommended,
  {
    files: ['**/*.ts', '**/*.mts', '**/*.cts', '**/*.tsx'],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module'
      }
    },
    plugins: {
      '@typescript-eslint': tseslint
    },
    rules: {
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': 'off',
      '@typescript-eslint/no-explicit-any': 'error',
      'no-undef': 'off'
    }
  },
  {
    languageOptions: {
      globals: {
        AbortController: 'readonly',
        Headers: 'readonly',
        RequestInit: 'readonly',
        URL: 'readonly',
        URLSearchParams: 'readonly',
        clearInterval: 'readonly',
        console: 'readonly',
        fetch: 'readonly',
        process: 'readonly',
        setInterval: 'readonly',
        setTimeout: 'readonly'
      }
    }
  }
];
