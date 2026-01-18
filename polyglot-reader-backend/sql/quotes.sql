-- Quotes table for the Home screen quote card.
-- Compatible with Supabase Postgres.

create table if not exists public.quotes (
  id bigserial primary key,
  language_code text not null,
  text text not null,
  translation text,
  author text,
  source text,
  created_at timestamptz not null default now(),
  constraint quotes_language_text_unique unique (language_code, text)
);

create index if not exists quotes_language_code_idx on public.quotes(language_code);

-- Optional: seed a few starter quotes (safe defaults). Replace with your own curated set.
-- NOTE: For production you should review licensing/attribution for any quote content you store.
insert into public.quotes (language_code, text, translation, author, source)
values
  ('de', 'Eine Folge von kleinen Willensakten liefert ein bedeutendes Ergebnis.', 'A series of small acts of will produces a significant result.', 'Charles Baudelaire', null),
  ('fr', 'La patience est amère, mais son fruit est doux.', 'Patience is bitter, but its fruit is sweet.', 'Jean-Jacques Rousseau', null),
  ('es', 'El que lee mucho y anda mucho, ve mucho y sabe mucho.', 'He who reads much and walks much, sees much and knows much.', 'Miguel de Cervantes', null),
  ('en', 'Little by little, a little becomes a lot.', null, 'Tanzanian proverb', null),
  ('it', 'Chi va piano va sano e va lontano.', 'Slowly does it: go slowly and you will go far.', 'Italian proverb', null),
  ('ru', 'Повторение — мать учения.', 'Repetition is the mother of learning.', 'Russian proverb', null),
  ('bn', 'যদি তোর ডাক শুনে কেউ না আসে তবে একলা চলো রে।', 'If no one responds to your call, then walk alone.', 'Rabindranath Tagore', null)
on conflict (language_code, text) do nothing;

