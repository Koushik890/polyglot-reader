-- Quote-of-the-day v2:
-- - Store a curated set of English "learning/reading/vocabulary" quotes (quote_bases)
-- - Cache daily translations per language (quote_daily_translations)
-- - Backend translates using configured translation provider (Google Translate API or LibreTranslate)

create table if not exists public.quote_bases (
  id bigserial primary key,
  text_en text not null,
  author text,
  source text,
  created_at timestamptz not null default now(),
  constraint quote_bases_text_unique unique (text_en)
);

create index if not exists quote_bases_created_at_idx on public.quote_bases(created_at);

create table if not exists public.quote_daily_translations (
  date_key text not null, -- YYYY-MM-DD (UTC)
  lang_code text not null, -- e.g. "de", "es", "bn"
  base_id bigint not null references public.quote_bases(id) on delete cascade,
  text text not null,
  created_at timestamptz not null default now(),
  primary key (date_key, lang_code)
);

create index if not exists quote_daily_translations_base_id_idx on public.quote_daily_translations(base_id);

-- Which base quote is selected for a given day (global, shared across users)
create table if not exists public.quote_daily_picks (
  date_key text primary key, -- YYYY-MM-DD (UTC)
  base_id bigint not null references public.quote_bases(id) on delete cascade,
  provider text,
  provider_quote_id text,
  created_at timestamptz not null default now()
);

create index if not exists quote_daily_picks_base_id_idx on public.quote_daily_picks(base_id);

-- Seed: learning-focused phrases (original / generic). Author optional.
insert into public.quote_bases (text_en, author, source)
values
  ('Read a little every day; consistency beats intensity.', null, 'Polyglot Reader'),
  ('Each page is practice. Each word is progress.', null, 'Polyglot Reader'),
  ('Save the words you meet. Review the words you keep.', null, 'Polyglot Reader'),
  ('Don''t chase perfection—chase comprehension.', null, 'Polyglot Reader'),
  ('A small daily habit can build a big vocabulary.', null, 'Polyglot Reader'),
  ('Make reading your classroom, and curiosity your teacher.', null, 'Polyglot Reader'),
  ('Slow reading today becomes fluent reading tomorrow.', null, 'Polyglot Reader'),
  ('Learn the word in the sentence, not the sentence in the word.', null, 'Polyglot Reader'),
  ('Highlight what matters. Revisit what you forget.', null, 'Polyglot Reader'),
  ('The best book is the one you finish.', null, 'Polyglot Reader'),
  ('If you can understand the story, you can learn the language.', null, 'Polyglot Reader'),
  ('New words are friends you haven''t met yet.', null, 'Polyglot Reader'),
  ('Read first for meaning, then for mastery.', null, 'Polyglot Reader'),
  ('One paragraph a day is still a paragraph ahead.', null, 'Polyglot Reader'),
  ('Translate less over time; understand more over time.', null, 'Polyglot Reader'),
  ('Your library is your language gym.', null, 'Polyglot Reader'),
  ('Repetition turns recognition into recall.', null, 'Polyglot Reader'),
  ('A bookmark is a promise to return.', null, 'Polyglot Reader'),
  ('The goal is not speed—it''s understanding.', null, 'Polyglot Reader'),
  ('Reading is the quiet way to learn loudly.', null, 'Polyglot Reader')
on conflict (text_en) do nothing;

