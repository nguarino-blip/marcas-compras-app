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
