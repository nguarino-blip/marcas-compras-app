-- =============================================
-- CDimex Marcas-Compras App - Database Setup
-- Run this in Supabase Dashboard > SQL Editor
-- =============================================

-- 1. Enable extensions
create extension if not exists "uuid-ossp";

-- 2. Profiles table (extends Supabase auth.users)
create table public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text unique not null,
  full_name text not null,
  role text not null check (role in ('marcas','compras','comex','calidad','admin')),
  sub_roles text[] default '{}',
  can_approve_solicitud boolean default false,
  active boolean default true,
  created_at timestamptz default now()
);

-- 3. Solicitudes
create table public.solicitudes (
  id uuid primary key default uuid_generate_v4(),
  numero serial unique,
  tipo text not null check (tipo in ('Nuevos desarrollos','Cambio de proveedor','Traspaso compra local al exterior','Compras regulares')),
  marca text not null,
  nombre text not null,
  descripcion text,
  fecha_lanzamiento date,
  fecha_sell_in date,
  costo_objetivo numeric,
  cotizacion_responsable text check (cotizacion_responsable in ('compras','marcas','conjunto')),
  cotizacion_responsable_nombre text,
  links_referencia text[] default '{}',
  status text not null default 'Pendiente aprobación Compras',
  created_by uuid references public.profiles(id),
  assigned_to uuid references public.profiles(id),
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- 4. Items de solicitud
create table public.solicitud_items (
  id uuid primary key default uuid_generate_v4(),
  solicitud_id uuid references public.solicitudes(id) on delete cascade,
  nombre text not null,
  tipo_producto text check (tipo_producto in ('Frasco vidrio','Frasco plástico','Estuche','Tapa','Válvula','Etiqueta','Otro')),
  status_estetico text default 'pendiente' check (status_estetico in ('pendiente','aprobado','rechazado')),
  status_tecnico text default 'pendiente' check (status_tecnico in ('pendiente','aprobado','rechazado')),
  aprobado_estetico_by uuid references public.profiles(id),
  aprobado_estetico_at timestamptz,
  aprobado_tecnico_by uuid references public.profiles(id),
  aprobado_tecnico_at timestamptz,
  compra_parcial boolean default false,
  compra_parcial_detalle text,
  -- Tratamiento especial: estuches
  tratamiento_especial text check (tratamiento_especial in ('estuche','esencia',null)),
  presupuesto_estuche numeric,
  muestra_estuche_aprobada boolean,
  -- Tratamiento especial: esencias
  esencia_porcentaje numeric,
  esencia_tipo text,
  esencia_legal_status text check (esencia_legal_status in ('pendiente','aprobado','rechazado',null)),
  notas text,
  created_at timestamptz default now()
);

-- 5. Retroplan pasos
create table public.retroplan_pasos (
  id uuid primary key default uuid_generate_v4(),
  solicitud_id uuid references public.solicitudes(id) on delete cascade,
  orden int not null,
  nombre text not null,
  responsable text,
  dias_offset int not null default 0,
  fecha_objetivo date,
  fecha_completado date,
  completado boolean default false,
  notas text,
  created_at timestamptz default now()
);

-- 6. Inspecciones pre-embarque
create table public.inspecciones (
  id uuid primary key default uuid_generate_v4(),
  solicitud_id uuid references public.solicitudes(id) on delete cascade,
  tipo text not null check (tipo in ('calidad_funcional','estetica_marcas','comex_packing')),
  resultado text default 'pendiente' check (resultado in ('pendiente','aprobado','desaprobado')),
  aprobado_by uuid references public.profiles(id),
  aprobado_at timestamptz,
  notas text,
  archivos text[] default '{}',
  created_at timestamptz default now()
);

-- 7. Comex seguimiento
create table public.comex_seguimiento (
  id uuid primary key default uuid_generate_v4(),
  solicitud_id uuid unique references public.solicitudes(id) on delete cascade,
  op_numero text,
  pago_exterior_status text default 'pendiente',
  pago_exterior_fecha date,
  packing_list_ok boolean default false,
  shipping_marks_ok boolean default false,
  documentacion jsonb default '{}',
  google_sheet_row text,
  notas text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- 8. Comentarios
create table public.comentarios (
  id uuid primary key default uuid_generate_v4(),
  solicitud_id uuid references public.solicitudes(id) on delete cascade,
  autor_id uuid references public.profiles(id),
  texto text not null,
  created_at timestamptz default now()
);

-- 9. Historial de eventos
create table public.historial (
  id uuid primary key default uuid_generate_v4(),
  solicitud_id uuid references public.solicitudes(id) on delete cascade,
  actor_id uuid references public.profiles(id),
  evento text not null,
  detalle text,
  created_at timestamptz default now()
);

-- 10. Notificaciones pendientes (para recordatorios)
create table public.notificaciones (
  id uuid primary key default uuid_generate_v4(),
  solicitud_id uuid references public.solicitudes(id) on delete cascade,
  tipo text not null check (tipo in ('recordatorio_20d','recordatorio_10d','vencido','cambio_fecha')),
  destinatario_email text not null,
  enviado boolean default false,
  enviado_at timestamptz,
  created_at timestamptz default now()
);

-- =============================================
-- ROW LEVEL SECURITY
-- =============================================

alter table public.profiles enable row level security;
alter table public.solicitudes enable row level security;
alter table public.solicitud_items enable row level security;
alter table public.retroplan_pasos enable row level security;
alter table public.inspecciones enable row level security;
alter table public.comex_seguimiento enable row level security;
alter table public.comentarios enable row level security;
alter table public.historial enable row level security;
alter table public.notificaciones enable row level security;

-- Profiles: users can read all profiles, update own
create policy "Profiles: read all" on public.profiles for select to authenticated using (true);
create policy "Profiles: update own" on public.profiles for update to authenticated using (auth.uid() = id);
create policy "Profiles: insert own" on public.profiles for insert to authenticated with check (auth.uid() = id);

-- Solicitudes: all authenticated can read, marcas can insert, role-based update
create policy "Solicitudes: read all" on public.solicitudes for select to authenticated using (true);
create policy "Solicitudes: marcas insert" on public.solicitudes for insert to authenticated
  with check (
    exists (select 1 from public.profiles where id = auth.uid() and role = 'marcas')
  );
create policy "Solicitudes: update by role" on public.solicitudes for update to authenticated using (true);

-- Items: all read, role-based insert/update
create policy "Items: read all" on public.solicitud_items for select to authenticated using (true);
create policy "Items: insert" on public.solicitud_items for insert to authenticated with check (true);
create policy "Items: update" on public.solicitud_items for update to authenticated using (true);

-- Retroplan: all read, compras insert/update
create policy "Retroplan: read all" on public.retroplan_pasos for select to authenticated using (true);
create policy "Retroplan: insert" on public.retroplan_pasos for insert to authenticated with check (true);
create policy "Retroplan: update" on public.retroplan_pasos for update to authenticated using (true);
create policy "Retroplan: delete" on public.retroplan_pasos for delete to authenticated using (true);

-- Inspecciones: all read, role-based insert/update
create policy "Inspecciones: read all" on public.inspecciones for select to authenticated using (true);
create policy "Inspecciones: insert" on public.inspecciones for insert to authenticated with check (true);
create policy "Inspecciones: update" on public.inspecciones for update to authenticated using (true);

-- Comex: all read, comex insert/update
create policy "Comex: read all" on public.comex_seguimiento for select to authenticated using (true);
create policy "Comex: insert" on public.comex_seguimiento for insert to authenticated with check (true);
create policy "Comex: update" on public.comex_seguimiento for update to authenticated using (true);

-- Comentarios: all read, authenticated insert
create policy "Comentarios: read all" on public.comentarios for select to authenticated using (true);
create policy "Comentarios: insert" on public.comentarios for insert to authenticated with check (auth.uid() = autor_id);

-- Historial: all read, authenticated insert
create policy "Historial: read all" on public.historial for select to authenticated using (true);
create policy "Historial: insert" on public.historial for insert to authenticated with check (true);

-- Notificaciones: service role only (API routes handle these)
create policy "Notificaciones: read all" on public.notificaciones for select to authenticated using (true);
create policy "Notificaciones: insert" on public.notificaciones for insert to authenticated with check (true);
create policy "Notificaciones: update" on public.notificaciones for update to authenticated using (true);

-- =============================================
-- TRIGGERS
-- =============================================

-- Auto-update updated_at on solicitudes
create or replace function public.handle_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create trigger on_solicitud_updated
  before update on public.solicitudes
  for each row execute function public.handle_updated_at();

create trigger on_comex_updated
  before update on public.comex_seguimiento
  for each row execute function public.handle_updated_at();

-- Auto-create profile on signup with correct role
create or replace function public.handle_new_user()
returns trigger as $$
declare
  v_role text;
  v_sub_roles text[];
  v_can_approve boolean;
  v_name text;
  v_email text;
begin
  v_email := lower(new.raw_user_meta_data->>'email');
  if v_email is null then
    v_email := lower(new.email);
  end if;
  v_name := coalesce(new.raw_user_meta_data->>'full_name', split_part(v_email, '@', 1));

  -- Default role
  v_role := 'marcas';
  v_sub_roles := '{}';
  v_can_approve := false;

  -- Assign role based on email
  case v_email
    -- COMPRAS team
    when 'lruiz@cdimex.com.ar' then v_role:='compras'; v_sub_roles:='{"comex"}'; v_name:='Lautaro Ruiz';
    when 'tschuster@cdimex.com.ar' then v_role:='compras'; v_name:='Thomas Schuster';
    when 'spereyra@cdimex.com.ar' then v_role:='compras'; v_name:='Sabrina Pereyra';
    when 'mjaimes@cdimex.com.ar' then v_role:='compras'; v_can_approve:=true; v_name:='Mariángeles Jaimes';
    when 'amartinez@cdimex.com.ar' then v_role:='compras'; v_name:='Agostina Martinez';
    when 'ncamusso@cdimex.com.ar' then v_role:='compras'; v_name:='Natalia Camusso';
    when 'dmenelli@cdimex.com.ar' then v_role:='compras'; v_name:='Daiana Menelli';
    when 'abenitez@cdimex.com.ar' then v_role:='compras'; v_sub_roles:='{"calidad"}'; v_name:='Alejandro Benitez';
    when 'dseravalle@cdimex.com.ar' then v_role:='compras'; v_name:='Diego Andrés Seravalle';
    when 'nmaggi@cdimex.com.ar' then v_role:='compras'; v_sub_roles:='{"calidad"}'; v_can_approve:=true; v_name:='Nadia Maggi';
    when 'nroldan@cdimex.com.ar' then v_role:='compras'; v_sub_roles:='{"comex"}'; v_name:='Noelia Roldan';
    when 'nguarino@cdimex.com.ar' then v_role:='compras'; v_can_approve:=true; v_name:='Nicolás Guarino';
    -- MARCAS team
    when 'fbarrionuevo@cdimex.com.ar' then v_role:='marcas'; v_name:='Florencia Barrionuevo';
    when 'cmanoleay@cdimex.com.ar' then v_role:='marcas'; v_name:='Camila Manoleay';
    when 'nmourelle@cdimex.com.ar' then v_role:='marcas'; v_name:='Nicole Mourelle';
    when 'vsolis@cdimex.com.ar' then v_role:='marcas'; v_name:='Valeria Solis';
    when 'canriquez@cdimex.com.ar' then v_role:='marcas'; v_name:='Carla Anriquez';
    when 'amosquera@cdimex.com.ar' then v_role:='marcas'; v_name:='Agustina Mosquera';
    when 'mkovacs@cdimex.com.ar' then v_role:='marcas'; v_name:='Macarena Kovacs';
    when 'icortina@cdimex.com.mx' then v_role:='marcas'; v_name:='Ignacio Cortina';
    when 'rderiso@cdimex.com.ar' then v_role:='marcas'; v_name:='Rosario del Riso';
    when 'glaffitte@cdimex.com.ar' then v_role:='marcas'; v_name:='Geraldine Laffitte';
    when 'fcortassa@cdimex.com.ar' then v_role:='marcas'; v_name:='Franco Cortassa';
    else
      v_role := 'marcas'; -- default for unknown emails
  end case;

  insert into public.profiles (id, email, full_name, role, sub_roles, can_approve_solicitud)
  values (new.id, v_email, v_name, v_role, v_sub_roles, v_can_approve);

  return new;
end;
$$ language plpgsql security definer;

-- Drop if exists, then create
drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();

-- =============================================
-- HELPER FUNCTIONS
-- =============================================

-- Get pending reminders (called by cron API route)
create or replace function public.get_upcoming_deadlines()
returns table (
  solicitud_id uuid,
  solicitud_nombre text,
  solicitud_numero int,
  paso_nombre text,
  fecha_objetivo date,
  dias_restantes int,
  responsable text,
  created_by_email text
) as $$
begin
  return query
  select
    s.id,
    s.nombre,
    s.numero::int,
    rp.nombre as paso_nombre,
    rp.fecha_objetivo,
    (rp.fecha_objetivo - current_date)::int as dias_restantes,
    rp.responsable,
    p.email as created_by_email
  from public.retroplan_pasos rp
  join public.solicitudes s on s.id = rp.solicitud_id
  join public.profiles p on p.id = s.created_by
  where rp.completado = false
    and rp.fecha_objetivo is not null
    and (rp.fecha_objetivo - current_date) in (20, 10, 0, -1)
    and s.status not in ('Completado','Rechazada')
  order by rp.fecha_objetivo asc;
end;
$$ language plpgsql security definer;

-- Get overdue steps for weekly report
create or replace function public.get_weekly_report_data()
returns json as $$
declare
  result json;
begin
  select json_build_object(
    'total_activas', (select count(*) from solicitudes where status not in ('Completado','Rechazada')),
    'vencidas', (
      select json_agg(row_to_json(t)) from (
        select s.numero, s.nombre, s.marca, rp.nombre as paso, rp.fecha_objetivo,
               (current_date - rp.fecha_objetivo)::int as dias_vencido
        from retroplan_pasos rp
        join solicitudes s on s.id = rp.solicitud_id
        where rp.completado = false and rp.fecha_objetivo < current_date
          and s.status not in ('Completado','Rechazada')
        order by rp.fecha_objetivo asc
      ) t
    ),
    'proximas_10d', (
      select json_agg(row_to_json(t)) from (
        select s.numero, s.nombre, s.marca, rp.nombre as paso, rp.fecha_objetivo,
               (rp.fecha_objetivo - current_date)::int as dias_restantes
        from retroplan_pasos rp
        join solicitudes s on s.id = rp.solicitud_id
        where rp.completado = false
          and rp.fecha_objetivo between current_date and current_date + 10
          and s.status not in ('Completado','Rechazada')
        order by rp.fecha_objetivo asc
      ) t
    ),
    'completadas_semana', (
      select count(*) from solicitudes
      where status = 'Completado'
        and updated_at >= current_date - interval '7 days'
    )
  ) into result;
  return result;
end;
$$ language plpgsql security definer;

-- =============================================
-- USUARIO-MARCAS MAPPING TABLE
-- =============================================
create table public.usuario_marcas (
  id uuid primary key default uuid_generate_v4(),
  user_id uuid references public.profiles(id) on delete cascade,
  marca text not null,
  created_at timestamptz default now(),
  unique(user_id, marca)
);

alter table public.usuario_marcas enable row level security;
create policy "usuario_marcas: read all" on public.usuario_marcas for select to authenticated using (true);
create policy "usuario_marcas: admin insert" on public.usuario_marcas for insert to authenticated
  with check (exists (select 1 from public.profiles where id = auth.uid() and role = 'admin'));
create policy "usuario_marcas: admin delete" on public.usuario_marcas for delete to authenticated
  using (exists (select 1 from public.profiles where id = auth.uid() and role = 'admin'));
create policy "usuario_marcas: admin update" on public.usuario_marcas for update to authenticated
  using (exists (select 1 from public.profiles where id = auth.uid() and role = 'admin'));

-- =============================================
-- PLANTILLAS POR MARCA (proveedores habituales)
-- =============================================
create table public.plantillas_marca (
  id uuid primary key default uuid_generate_v4(),
  marca text not null,
  proveedores_habituales text[] default '{}',
  updated_at timestamptz default now(),
  unique(marca)
);

alter table public.plantillas_marca enable row level security;
create policy "plantillas_marca: read all" on public.plantillas_marca for select to authenticated using (true);
create policy "plantillas_marca: admin insert" on public.plantillas_marca for insert to authenticated
  with check (exists (select 1 from public.profiles where id = auth.uid() and role = 'admin'));
create policy "plantillas_marca: admin update" on public.plantillas_marca for update to authenticated
  using (exists (select 1 from public.profiles where id = auth.uid() and role = 'admin'));
create policy "plantillas_marca: admin delete" on public.plantillas_marca for delete to authenticated
  using (exists (select 1 from public.profiles where id = auth.uid() and role = 'admin'));

-- =============================================
-- SEED: Initial usuario_marcas data (migrate from hardcoded map)
-- =============================================
create or replace function public.seed_usuario_marcas()
returns void as $$
declare
  v_uid uuid;
begin
  -- Florencia Barrionuevo → Cher Fragancias
  select id into v_uid from profiles where email = 'fbarrionuevo@cdimex.com.ar';
  if v_uid is not null then insert into usuario_marcas(user_id, marca) values (v_uid, 'Cher Fragancias') on conflict do nothing; end if;

  -- Agustina Mosquera → Cher Beauty
  select id into v_uid from profiles where email = 'amosquera@cdimex.com.ar';
  if v_uid is not null then insert into usuario_marcas(user_id, marca) values (v_uid, 'Cher Beauty') on conflict do nothing; end if;

  -- Macarena Kovacs → Cher Fragancias, Cher Beauty
  select id into v_uid from profiles where email = 'mkovacs@cdimex.com.ar';
  if v_uid is not null then
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Cher Fragancias') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Cher Beauty') on conflict do nothing;
  end if;

  -- Camila Manoleay → Masivas
  select id into v_uid from profiles where email = 'cmanoleay@cdimex.com.ar';
  if v_uid is not null then
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Al Shams') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'L''Inedito') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Get the Look') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Oreiro Love') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Margarita') on conflict do nothing;
  end if;

  -- Nicole Mourelle → Masivas
  select id into v_uid from profiles where email = 'nmourelle@cdimex.com.ar';
  if v_uid is not null then
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Al Shams') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'L''Inedito') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Get the Look') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Oreiro Love') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Margarita') on conflict do nothing;
  end if;

  -- Rosario del Riso → Masivas (BM)
  select id into v_uid from profiles where email = 'rderiso@cdimex.com.ar';
  if v_uid is not null then
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Al Shams') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'L''Inedito') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Get the Look') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Oreiro Love') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Margarita') on conflict do nothing;
  end if;

  -- Valeria Solis → Bensimon, Tucci
  select id into v_uid from profiles where email = 'vsolis@cdimex.com.ar';
  if v_uid is not null then
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Bensimon') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Tucci') on conflict do nothing;
  end if;

  -- Carla Anriquez → Sarkany
  select id into v_uid from profiles where email = 'canriquez@cdimex.com.ar';
  if v_uid is not null then insert into usuario_marcas(user_id, marca) values (v_uid, 'Sarkany') on conflict do nothing; end if;

  -- Ignacio Cortina → BM Bensimon/Sarkany/Tucci
  select id into v_uid from profiles where email = 'icortina@cdimex.com.mx';
  if v_uid is not null then
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Bensimon') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Sarkany') on conflict do nothing;
    insert into usuario_marcas(user_id, marca) values (v_uid, 'Tucci') on conflict do nothing;
  end if;

  -- Geraldine Laffitte → K-beauty
  select id into v_uid from profiles where email = 'glaffitte@cdimex.com.ar';
  if v_uid is not null then insert into usuario_marcas(user_id, marca) values (v_uid, 'K-beauty') on conflict do nothing; end if;

  -- Franco Cortassa → __ALL__ (Director General)
  select id into v_uid from profiles where email = 'fcortassa@cdimex.com.ar';
  if v_uid is not null then insert into usuario_marcas(user_id, marca) values (v_uid, '__ALL__') on conflict do nothing; end if;
end;
$$ language plpgsql security definer;

-- Execute: SELECT seed_usuario_marcas();

-- =============================================
-- SMART ALERTS: stale solicitudes + overdue inspections
-- =============================================
create or replace function public.get_smart_alerts()
returns json as $$
declare
  result json;
begin
  select json_build_object(
    'sin_movimiento', (
      select json_agg(row_to_json(t)) from (
        select s.numero, s.nombre, s.marca, s.status,
               (current_date - s.updated_at::date)::int as dias_sin_movimiento,
               p.email as created_by_email
        from solicitudes s
        join profiles p on p.id = s.created_by
        where s.status not in ('Completado','Rechazada','Eliminada')
          and (current_date - s.updated_at::date) >= 5
        order by s.updated_at asc
      ) t
    ),
    'inspeccion_vencida', (
      select json_agg(row_to_json(t)) from (
        select s.numero, s.nombre, s.marca,
               i.tipo, i.notas,
               i.created_at::date as fecha_desaprobacion,
               p.email as created_by_email
        from inspecciones i
        join solicitudes s on s.id = i.solicitud_id
        join profiles p on p.id = s.created_by
        where i.resultado = 'desaprobado'
          and s.status = 'Inspección pre-embarque'
          and i.notas like '%[PLAZO:%'
        order by i.created_at asc
      ) t
    ),
    'paso_excede_promedio', (
      select json_agg(row_to_json(t)) from (
        select s.numero, s.nombre, s.marca, rp.nombre as paso,
               rp.fecha_objetivo,
               (current_date - rp.fecha_objetivo)::int as dias_excedido,
               p.email as created_by_email
        from retroplan_pasos rp
        join solicitudes s on s.id = rp.solicitud_id
        join profiles p on p.id = s.created_by
        where rp.completado = false
          and rp.fecha_objetivo < current_date
          and s.status not in ('Completado','Rechazada','Eliminada')
        order by (current_date - rp.fecha_objetivo) desc
        limit 20
      ) t
    )
  ) into result;
  return result;
end;
$$ language plpgsql security definer;

-- =============================================
-- METRICS: analytics dashboard data
-- =============================================
create or replace function public.get_metrics_data()
returns json as $$
declare
  result json;
begin
  select json_build_object(
    'tiempo_por_estado', (
      select json_agg(row_to_json(t)) from (
        select h1.evento as estado_desde,
               avg(extract(epoch from (h2.created_at - h1.created_at))/86400)::numeric(10,1) as dias_promedio,
               count(*) as cantidad
        from historial h1
        join historial h2 on h1.solicitud_id = h2.solicitud_id
          and h2.created_at > h1.created_at
          and h2.evento like 'Cambió estado a%'
        where h1.evento like 'Cambió estado a%'
        group by h1.evento
        having count(*) >= 2
        order by dias_promedio desc
      ) t
    ),
    'por_marca', (
      select json_agg(row_to_json(t)) from (
        select s.marca, count(*) as total,
               count(*) filter (where s.status = 'Completado') as completadas,
               count(*) filter (where s.status not in ('Completado','Rechazada','Eliminada')) as activas
        from solicitudes s
        group by s.marca order by total desc
      ) t
    ),
    'por_tipo', (
      select json_agg(row_to_json(t)) from (
        select s.tipo, count(*) as total,
               count(*) filter (where s.status = 'Completado') as completadas,
               count(*) filter (where s.status not in ('Completado','Rechazada','Eliminada')) as activas
        from solicitudes s
        group by s.tipo order by total desc
      ) t
    ),
    'tendencia_mensual', (
      select json_agg(row_to_json(t)) from (
        select to_char(date_trunc('month', created_at), 'YYYY-MM') as mes,
               count(*) as creadas,
               count(*) filter (where status = 'Completado') as completadas
        from solicitudes
        where created_at >= current_date - interval '12 months'
        group by date_trunc('month', created_at) order by mes
      ) t
    ),
    'cuellos_botella', (
      select json_agg(row_to_json(t)) from (
        select rp.nombre as paso,
               avg((current_date - rp.fecha_objetivo)::int)::numeric(10,1) as dias_promedio_retraso,
               count(*) as cantidad_retrasados
        from retroplan_pasos rp
        join solicitudes s on s.id = rp.solicitud_id
        where rp.completado = false and rp.fecha_objetivo < current_date
          and s.status not in ('Completado','Rechazada','Eliminada')
        group by rp.nombre having count(*) >= 1
        order by dias_promedio_retraso desc
      ) t
    )
  ) into result;
  return result;
end;
$$ language plpgsql security definer;

-- =============================================
-- STOCK & PLANIFICACIÓN DE COMPRAS
-- =============================================

-- Config por marca: lead time, umbral de alerta
create table if not exists config_marcas (
  id uuid default gen_random_uuid() primary key,
  marca text not null unique,
  lead_time_meses numeric default 3,
  umbral_alerta_meses numeric default 2,
  tipo_abastecimiento text default 'fabricacion', -- fabricacion | compra
  notas text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table config_marcas enable row level security;
create policy "config_marcas_read" on config_marcas for select using (true);
create policy "config_marcas_write" on config_marcas for all using (
  exists(select 1 from profiles where id = auth.uid() and (role = 'admin' or (role = 'compras' and can_approve_solicitud = true)))
);

-- Stock de productos sincronizado desde Google Sheets
create table if not exists stock_productos (
  id uuid default gen_random_uuid() primary key,
  codigo text not null,
  nombre text not null,
  marca text not null,
  segmento text,
  polo text,
  stock_actual numeric default 0,
  venta_mensual_avg numeric default 0,
  forecast_proximo_mes numeric default 0,
  cobertura_meses numeric default 0,
  lead_time_meses numeric default null, -- null = usa default de marca
  stock_minimo numeric default 0,
  unidad text default 'unidades',
  fecha_sync timestamptz default now(),
  unique(codigo, marca)
);

alter table stock_productos enable row level security;
create policy "stock_productos_read" on stock_productos for select using (true);
create policy "stock_productos_write" on stock_productos for all using (
  exists(select 1 from profiles where id = auth.uid() and (role = 'admin' or role = 'compras'))
);

-- Historial de sincronizaciones
create table if not exists stock_sync_log (
  id uuid default gen_random_uuid() primary key,
  fecha timestamptz default now(),
  productos_actualizados int default 0,
  errores text,
  fuente text default 'google_sheets'
);

-- Config global de Google Sheets (una sola fila)
create table if not exists config_sheets (
  id int primary key default 1 check (id = 1),
  sheet_id_stocks text,
  sheet_name_stocks text default 'Ventas stock cierre cobertura',
  sheet_id_forecast text,
  sheet_name_forecast text default 'Resumen Gral.',
  sheet_id_stock_sistema text,
  sheet_name_stock_sistema text default 'Stock Sistema',
  sheet_name_descripcion text default 'Descripcion',
  sheet_id_bom text,
  sheet_name_bom text default 'Nueva base bruta',
  sheet_id_producciones text,
  sheet_name_producciones text default 'UNIFICADO',
  service_account_email text,
  last_sync timestamptz,
  sync_enabled boolean default true
);

alter table config_sheets enable row level security;
create policy "config_sheets_read" on config_sheets for select using (true);
create policy "config_sheets_write" on config_sheets for all using (
  exists(select 1 from profiles where id = auth.uid() and (role = 'admin' or (role = 'compras' and can_approve_solicitud = true)))
);

-- Seed config_marcas con las marcas existentes
insert into config_marcas (marca, lead_time_meses, umbral_alerta_meses, tipo_abastecimiento) values
  -- Fabricación propia (insumos: frascos 4.5m, estuches 4m, esencias 1-2m)
  ('Bensimon', 4.5, 2, 'fabricacion'),
  ('Sarkany', 4.5, 2, 'fabricacion'),
  ('Tucci', 4.5, 2, 'fabricacion'),
  ('Cher Fragancias', 4.5, 2, 'fabricacion'),
  ('Cher Mix', 4.5, 2, 'fabricacion'),
  ('Oreiro Love', 4.5, 2, 'fabricacion'),
  ('Al Shams', 4.5, 2, 'fabricacion'),
  ('L''Inedito', 4.5, 2, 'fabricacion'),
  ('Margarita', 4.5, 2, 'fabricacion'),
  ('Little Paris', 4.5, 2, 'fabricacion'),
  ('Relazzi', 4.5, 2, 'fabricacion'),
  -- Compra hecha - Árabes (6 meses)
  ('Lattafa', 6, 2, 'compra'),
  ('Al Wataniah', 6, 2, 'compra'),
  ('Rasasi', 6, 2, 'compra'),
  ('Armaf', 6, 2, 'compra'),
  ('Afnan', 6, 2, 'compra'),
  -- Compra hecha - Otros
  ('Elizabeth Arden', 4, 2, 'compra'),
  ('Alchemy', 4, 2, 'compra'),
  ('Cher Beauty', 7, 2, 'compra'),
  -- Otros
  ('Fascino', 3, 2, 'fabricacion'),
  ('Get the Look', 3, 2, 'compra'),
  ('K-beauty', 3, 2, 'compra')
on conflict (marca) do nothing;

-- RPC: Obtener alertas de stock (items con riesgo de quiebre)
create or replace function get_stock_alerts_data(p_umbral numeric default null)
returns json as $$
declare
  result json;
begin
  select json_build_object(
    'resumen', (
      select json_build_object(
        'total_productos', count(*),
        'en_riesgo', count(*) filter (where sp.cobertura_meses <= coalesce(p_umbral, cm.umbral_alerta_meses, 2)),
        'sin_stock', count(*) filter (where sp.stock_actual <= 0),
        'cobertura_promedio', round(avg(sp.cobertura_meses)::numeric, 1)
      )
      from stock_productos sp
      left join config_marcas cm on cm.marca = sp.marca
    ),
    'alertas_quiebre', (
      select json_agg(row_to_json(t) order by t.cobertura_meses asc) from (
        select sp.codigo, sp.nombre, sp.marca, sp.segmento,
               sp.stock_actual, sp.venta_mensual_avg, sp.forecast_proximo_mes,
               sp.cobertura_meses,
               coalesce(sp.lead_time_meses, cm.lead_time_meses, 3) as lead_time,
               coalesce(p_umbral, cm.umbral_alerta_meses, 2) as umbral,
               case
                 when sp.stock_actual <= 0 then 'SIN STOCK'
                 when sp.cobertura_meses <= 1 then 'CRITICO'
                 when sp.cobertura_meses <= coalesce(p_umbral, cm.umbral_alerta_meses, 2) then 'ALERTA'
                 when sp.cobertura_meses <= coalesce(sp.lead_time_meses, cm.lead_time_meses, 3) then 'ATENCIÓN'
                 else 'OK'
               end as nivel_riesgo,
               round(greatest(0, coalesce(sp.lead_time_meses, cm.lead_time_meses, 3) * coalesce(nullif(sp.forecast_proximo_mes, 0), sp.venta_mensual_avg) - sp.stock_actual)::numeric, 0) as cantidad_sugerida_compra
        from stock_productos sp
        left join config_marcas cm on cm.marca = sp.marca
        where sp.cobertura_meses <= coalesce(sp.lead_time_meses, cm.lead_time_meses, 3)
           or sp.stock_actual <= 0
      ) t
    ),
    'por_marca', (
      select json_agg(row_to_json(t)) from (
        select sp.marca,
               count(*) as total_productos,
               count(*) filter (where sp.cobertura_meses <= coalesce(cm.umbral_alerta_meses, 2)) as en_riesgo,
               count(*) filter (where sp.stock_actual <= 0) as sin_stock,
               round(avg(sp.cobertura_meses)::numeric, 1) as cobertura_promedio,
               coalesce(cm.lead_time_meses, 3) as lead_time_default
        from stock_productos sp
        left join config_marcas cm on cm.marca = sp.marca
        group by sp.marca, cm.lead_time_meses
        order by en_riesgo desc
      ) t
    ),
    'fecha_sync', (select max(fecha_sync) from stock_productos)
  ) into result;
  return result;
end;
$$ language plpgsql security definer;

-- RPC: Recalcular cobertura después de sync
create or replace function recalculate_cobertura()
returns void as $$
begin
  update stock_productos
  set cobertura_meses = case
    when coalesce(nullif(forecast_proximo_mes, 0), venta_mensual_avg) > 0
    then round((stock_actual / coalesce(nullif(forecast_proximo_mes, 0), venta_mensual_avg))::numeric, 1)
    else 99
  end
  where venta_mensual_avg > 0 or forecast_proximo_mes > 0;
end;
$$ language plpgsql security definer;

-- =============================================
-- MRP: INSUMOS, BOM & PLANIFICACIÓN DE COMPRAS
-- =============================================

-- BOM: Bill of Materials (producto → insumos)
create table if not exists bom_productos (
  id uuid default gen_random_uuid() primary key,
  codigo_principal text not null,       -- código producto terminado
  nombre_principal text,
  nivel int not null default 0,         -- 0=principal, 1=insumo, 2=intermedio
  categoria text,                       -- PRINCIPAL, INSUMO, INTERMEDIO
  codigo_insumo text not null,          -- código del insumo/componente
  detalle_insumo text,
  cantidad_formula numeric default 1,   -- cuántas unidades de insumo por PT
  tipo_insumo text,                     -- UNICO, COMPARTIDO (N) - PRIO X, ES EL PRINCIPAL
  es_envase boolean default false,      -- true si el detalle contiene ENVASE/FRASCO
  fecha_sync timestamptz default now(),
  unique(codigo_principal, codigo_insumo)
);

alter table bom_productos enable row level security;
create policy "bom_read" on bom_productos for select using (true);
create policy "bom_write" on bom_productos for all using (
  exists(select 1 from profiles where id = auth.uid() and (role = 'admin' or role = 'compras'))
);

-- Stock de insumos
create table if not exists stock_insumos (
  id uuid default gen_random_uuid() primary key,
  codigo text not null unique,
  detalle text,
  categoria text,                       -- INSUMO, INTERMEDIO
  stock_fisico numeric default 0,
  stock_disponible numeric default 0,
  tipo_insumo_global text,              -- frasco, estuche, esencia, tapa, collar, valvula, otro
  lead_time_dias numeric default 135,   -- default 4.5 meses = 135 días
  fecha_sync timestamptz default now()
);

alter table stock_insumos enable row level security;
create policy "stock_insumos_read" on stock_insumos for select using (true);
create policy "stock_insumos_write" on stock_insumos for all using (
  exists(select 1 from profiles where id = auth.uid() and (role = 'admin' or role = 'compras'))
);

-- Producciones planificadas (estimadas, no confirmadas)
create table if not exists producciones_planificadas (
  id uuid default gen_random_uuid() primary key,
  codigo text not null,
  marca text,
  descripcion text,
  proveedor text,
  mes date not null,                    -- primer día del mes de producción
  cantidad numeric default 0,
  fecha_sync timestamptz default now(),
  unique(codigo, mes)
);

alter table producciones_planificadas enable row level security;
create policy "producciones_read" on producciones_planificadas for select using (true);
create policy "producciones_write" on producciones_planificadas for all using (
  exists(select 1 from profiles where id = auth.uid() and (role = 'admin' or role = 'compras'))
);

-- Agregar campos de sheets MRP a config_sheets
-- (ejecutar como ALTER si la tabla ya existe)
-- ALTER TABLE config_sheets ADD COLUMN IF NOT EXISTS sheet_id_bom text;
-- ALTER TABLE config_sheets ADD COLUMN IF NOT EXISTS sheet_name_bom text DEFAULT 'BASE BRUTA';
-- ALTER TABLE config_sheets ADD COLUMN IF NOT EXISTS sheet_id_producciones text;
-- ALTER TABLE config_sheets ADD COLUMN IF NOT EXISTS sheet_name_producciones text DEFAULT 'UNIFICADO';

-- RPC: Cálculo MRP — necesidades de compra de insumos
create or replace function get_mrp_analysis(p_meses_forecast int default 3)
returns json as $$
declare
  result json;
begin
  select json_build_object(
    'resumen', (
      select json_build_object(
        'total_pt', (select count(distinct codigo_principal) from bom_productos where nivel = 0),
        'total_insumos', (select count(*) from stock_insumos),
        'insumos_criticos', (
          select count(*) from stock_insumos si
          where si.stock_fisico <= 0
            and exists(select 1 from bom_productos b where b.codigo_insumo = si.codigo and b.es_envase = true)
        ),
        'fecha_sync', (select max(fecha_sync) from stock_insumos)
      )
    ),
    -- Análisis por producto terminado: stock, forecast, producción estimada, envase disponible, gap
    'por_producto', (
      select json_agg(row_to_json(t) order by t.gap_unidades desc) from (
        select
          sp.codigo,
          sp.nombre,
          sp.marca,
          sp.stock_actual as stock_pt,
          round((coalesce(nullif(sp.forecast_proximo_mes, 0), sp.venta_mensual_avg) * p_meses_forecast)::numeric, 0) as demanda_periodo,
          coalesce((
            select sum(pp.cantidad)
            from producciones_planificadas pp
            where pp.codigo = sp.codigo
              and pp.mes >= date_trunc('month', now())
              and pp.mes < date_trunc('month', now()) + (p_meses_forecast || ' months')::interval
          ), 0) as produccion_estimada,
          -- Envase: cuántas unidades puedo fabricar con stock de envases
          coalesce((
            select min(
              case when b.cantidad_formula > 0
                then floor(si.stock_fisico / b.cantidad_formula)
                else 999999
              end
            )
            from bom_productos b
            join stock_insumos si on si.codigo = b.codigo_insumo
            where b.codigo_principal = sp.codigo
              and b.es_envase = true
          ), 0) as capacidad_envase,
          -- Gap = demanda - stock - producción estimada
          greatest(0,
            round((coalesce(nullif(sp.forecast_proximo_mes, 0), sp.venta_mensual_avg) * p_meses_forecast)::numeric, 0)
            - sp.stock_actual
            - coalesce((
              select sum(pp.cantidad)
              from producciones_planificadas pp
              where pp.codigo = sp.codigo
                and pp.mes >= date_trunc('month', now())
                and pp.mes < date_trunc('month', now()) + (p_meses_forecast || ' months')::interval
            ), 0)
          ) as gap_unidades
        from stock_productos sp
        where sp.marca != 'Sin asignar'
          and (sp.venta_mensual_avg > 0 or sp.forecast_proximo_mes > 0)
        limit 500
      ) t
      where t.gap_unidades > 0 or t.capacidad_envase < t.demanda_periodo
    ),
    -- Compras sugeridas de insumos agrupadas por tipo
    'compras_sugeridas', (
      select json_agg(row_to_json(t) order by t.prioridad, t.tipo_insumo, t.gap_insumo desc) from (
        select
          si.codigo as codigo_insumo,
          si.detalle,
          si.tipo_insumo_global as tipo_insumo,
          si.stock_fisico as stock_insumo,
          si.lead_time_dias,
          -- Demanda total del insumo = sum de (gap_PT × cantidad_formula) para todos los PT que lo usan
          round(coalesce(sum(
            greatest(0,
              (coalesce(nullif(sp.forecast_proximo_mes, 0), sp.venta_mensual_avg) * p_meses_forecast)
              - sp.stock_actual
            ) * b.cantidad_formula
          ), 0)::numeric, 0) as demanda_insumo,
          -- Gap = demanda - stock actual del insumo
          greatest(0, round((coalesce(sum(
            greatest(0,
              (coalesce(nullif(sp.forecast_proximo_mes, 0), sp.venta_mensual_avg) * p_meses_forecast)
              - sp.stock_actual
            ) * b.cantidad_formula
          ), 0) - si.stock_fisico)::numeric, 0)) as gap_insumo,
          case si.tipo_insumo_global
            when 'frasco' then 1
            when 'estuche' then 2
            when 'esencia' then 3
            else 4
          end as prioridad,
          count(distinct b.codigo_principal) as productos_afectados
        from stock_insumos si
        join bom_productos b on b.codigo_insumo = si.codigo and b.nivel > 0
        join stock_productos sp on sp.codigo = b.codigo_principal and sp.marca != 'Sin asignar'
        where sp.venta_mensual_avg > 0 or sp.forecast_proximo_mes > 0
        group by si.codigo, si.detalle, si.tipo_insumo_global, si.stock_fisico, si.lead_time_dias
        having coalesce(sum(
          greatest(0,
            (coalesce(nullif(sp.forecast_proximo_mes, 0), sp.venta_mensual_avg) * p_meses_forecast)
            - sp.stock_actual
          ) * b.cantidad_formula
        ), 0) > si.stock_fisico
      ) t
    )
  ) into result;
  return result;
end;
$$ language plpgsql security definer;
