export type QuoteSeed = {
  languageCode: string;
  languageLabel: string;
  flagEmoji: string;
  text: string;
  translation?: string;
  author?: string;
  source?: string;
};

/**
 * Fallback quotes served if the database table isn't available or is empty.
 * Keep this list small and safe; the intended long-term source of truth is a DB table.
 */
export const FALLBACK_QUOTES: QuoteSeed[] = [
  {
    languageCode: 'de',
    languageLabel: 'German',
    flagEmoji: '🇩🇪',
    text: 'Eine Folge von kleinen Willensakten liefert ein bedeutendes Ergebnis.',
    translation: 'A series of small acts of will produces a significant result.',
    author: 'Charles Baudelaire',
  },
  {
    languageCode: 'fr',
    languageLabel: 'French',
    flagEmoji: '🇫🇷',
    text: 'La patience est amère, mais son fruit est doux.',
    translation: 'Patience is bitter, but its fruit is sweet.',
    author: 'Jean-Jacques Rousseau',
  },
  {
    languageCode: 'es',
    languageLabel: 'Spanish',
    flagEmoji: '🇪🇸',
    text: 'El que lee mucho y anda mucho, ve mucho y sabe mucho.',
    translation: 'He who reads much and walks much, sees much and knows much.',
    author: 'Miguel de Cervantes',
  },
  {
    languageCode: 'en',
    languageLabel: 'English',
    flagEmoji: '🇺🇸',
    text: 'Little by little, a little becomes a lot.',
    author: 'Tanzanian proverb',
  },
  {
    languageCode: 'it',
    languageLabel: 'Italian',
    flagEmoji: '🇮🇹',
    text: 'Chi va piano va sano e va lontano.',
    translation: 'Slowly does it: go slowly and you will go far.',
    author: 'Italian proverb',
  },
  {
    languageCode: 'ru',
    languageLabel: 'Russian',
    flagEmoji: '🇷🇺',
    text: 'Повторение — мать учения.',
    translation: 'Repetition is the mother of learning.',
    author: 'Russian proverb',
  },
  {
    languageCode: 'bn',
    languageLabel: 'Bengali',
    flagEmoji: '🇧🇩',
    text: 'যদি তোর ডাক শুনে কেউ না আসে তবে একলা চলো রে।',
    translation: 'If no one responds to your call, then walk alone.',
    author: 'Rabindranath Tagore',
  },
];

