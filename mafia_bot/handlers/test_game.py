# Telegram-facing self-play test mode.
# Starts a real domain game and replays its public/debug events into the
# current Telegram group so the game can be observed without real test users.
from ._context import *  # noqa: F401,F403

import traceback


TEST_GAME_TASKS: dict[int, asyncio.Task] = {}
TEST_GAME_ENABLED = os.getenv("TEST_GAME_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
TEST_GAME_DEFAULT_PLAYERS = max(4, min(20, int(os.getenv("TEST_GAME_PLAYERS", "10") or "10")))
TEST_GAME_DELAY = max(0.15, float(os.getenv("TEST_GAME_DELAY", "0.7") or "0.7"))
TEST_GAME_MAX_ROUNDS = max(10, int(os.getenv("TEST_GAME_MAX_ROUNDS", "40") or "40"))


def _test_game_allowed(user_id: int) -> bool:
    if user_id == OWNER_USER_ID:
        return True
    return user_id in read_user_id_set("TEST_GAME_USER_IDS")


def _test_role_label(role: str) -> str:
    return f"{ROLE_EMOJI.get(role, '')} {role}".strip()


def _test_targets(room, player, *, mafia_only: bool = False) -> list:
    targets = [
        candidate
        for candidate in room.alive_players()
        if candidate.user_id != player.user_id
    ]
    if mafia_only:
        safe = [candidate for candidate in targets if candidate.role not in MAFIA_ROLES]
        if safe:
            targets = safe
    return targets


def _test_pick(rng: random.Random, room, player, *, mafia_only: bool = False) -> int | None:
    targets = _test_targets(room, player, mafia_only=mafia_only)
    if not targets:
        return None
    return rng.choice(targets).user_id


async def _test_send(bot: Bot, chat_id: int, text: str) -> None:
    await bot.send_message(chat_id, text, parse_mode="HTML")


async def _simulate_one_day(room, bot: Bot, chat_id: int, rng: random.Random) -> list:
    room.phase = PHASE_DAY
    room.start_day_discussion()

    await _test_send(
        bot,
        chat_id,
        f"<b>☀️ ТЕСТ: {room.round_no} день — обсуждение</b>\n"
        "Автоматические игроки анализируют ночь...",
    )
    await asyncio.sleep(TEST_GAME_DELAY)

    room.start_day_nomination()
    await _test_send(bot, chat_id, "🗳 <b>ТЕСТ: начинается выбор кандидата.</b>")

    for player in list(room.alive_players()):
        target_id = _test_pick(rng, room, player, mafia_only=player.role in MAFIA_ROLES)
        if target_id is not None:
            ok, _ = room.set_day_vote(player.user_id, target_id)
            if not ok:
                raise RuntimeError(
                    f"Дневной голос не принят: player={player.user_id}, target={target_id}"
                )

    ok, candidate_id = room.resolve_day_nomination()
    if not ok:
        raise RuntimeError("Не удалось разрешить дневную номинацию.")

    if candidate_id is None:
        await _test_send(
            bot,
            chat_id,
            "🗿 <b>ТЕСТ:</b> голоса разделились. Сегодня никого не вешаем.",
        )
        ok, info = room.end_day_no_lynch()
        if not ok:
            raise RuntimeError(info)
        return []

    candidate = room.get_player(candidate_id)
    if candidate is None:
        raise RuntimeError(f"Номинация вернула неизвестного игрока: {candidate_id}")

    room.start_day_trial(candidate.user_id)
    await _test_send(
        bot,
        chat_id,
        f"⚖️ <b>ТЕСТ: суд над {candidate.full_name}</b>",
    )

    for voter in list(room.alive_players()):
        if voter.user_id == candidate.user_id:
            continue
        # Civilian-style bots are slightly more willing to lynch; mafia bots
        # oppose their own team only by construction of the candidate choice.
        approve_probability = 0.72 if voter.role not in MAFIA_ROLES else 0.60
        room.set_trial_vote(
            voter.user_id,
            rng.random() < approve_probability,
        )

    result = room.resolve_day_trial()
    ok, info, eliminated, don_note, don_successor_id, comm_note, comm_successor_id = result
    if not ok:
        raise RuntimeError(f"Дневное голосование не обработано: {info}")

    if eliminated:
        for player in eliminated:
            await _test_send(
                bot,
                chat_id,
                f"☠️ <b>ТЕСТ:</b> {player.full_name} выбыл днём. "
                f"Роль: {_test_role_label(player.role)}",
            )
        if don_note and don_successor_id is not None:
            successor = room.get_player(don_successor_id)
            await _test_send(
                bot,
                chat_id,
                f"🤵🏻 <b>ТЕСТ-наследование Дона:</b> "
                f"{successor.full_name if successor else don_successor_id}",
            )
        if comm_note and comm_successor_id is not None:
            successor = room.get_player(comm_successor_id)
            await _test_send(
                bot,
                chat_id,
                f"🕵️‍♂️ <b>ТЕСТ-наследование Комиссара:</b> "
                f"{successor.full_name if successor else comm_successor_id}",
            )
    else:
        await _test_send(
            bot,
            chat_id,
            "🕊 <b>ТЕСТ:</b> кандидата оставили в живых.",
        )

    await asyncio.sleep(TEST_GAME_DELAY)
    return eliminated


async def _simulate_one_night(room, bot: Bot, chat_id: int, rng: random.Random) -> list:
    if room.phase != PHASE_NIGHT:
        raise RuntimeError(f"Ожидалась ночь, сейчас {room.phase}")

    await _test_send(
        bot,
        chat_id,
        f"<b>🌙 ТЕСТ: {room.round_no} ночь</b>",
    )

    # Give every active role a valid automatic action.
    for player in list(room.alive_players()):
        if not player.alive:
            continue

        target_id = _test_pick(
            rng,
            room,
            player,
            mafia_only=player.role in MAFIA_ROLES,
        )

        if player.role in MAFIA_ROLES:
            if target_id is not None:
                room.set_night_vote(player.user_id, target_id)
        elif player.role == ROLE_DOCTOR:
            if target_id is not None:
                room.set_doctor_target(player.user_id, target_id)
        elif player.role == ROLE_MANIAC:
            if target_id is not None:
                room.set_maniac_target(player.user_id, target_id)
        elif player.role == ROLE_MISTRESS:
            if target_id is not None:
                ok, _ = room.set_mistress_target(player.user_id, target_id)
                if not ok:
                    alternatives = [
                        p for p in _test_targets(room, player)
                        if p.user_id != room.mistress_last_target_id
                    ]
                    if alternatives:
                        room.set_mistress_target(
                            player.user_id,
                            rng.choice(alternatives).user_id,
                        )
        elif player.role == ROLE_BUM:
            if target_id is not None:
                ok, _ = room.set_bum_target(player.user_id, target_id)
                if not ok:
                    alternatives = [
                        p for p in _test_targets(room, player)
                        if p.user_id != room.bum_last_target_id
                    ]
                    if alternatives:
                        room.set_bum_target(
                            player.user_id,
                            rng.choice(alternatives).user_id,
                        )
        elif player.role == ROLE_ADVOCATE:
            if target_id is not None:
                room.set_advocate_target(player.user_id, target_id)
        elif player.role == ROLE_COMMISSAR:
            if room.round_no >= 2 and rng.random() < 0.25:
                ok, _ = room.set_commissar_action_mode(player.user_id, "shoot")
                if ok:
                    shot_target = _test_pick(rng, room, player)
                    if shot_target is not None:
                        room.set_commissar_shot_target(player.user_id, shot_target)
            else:
                if room.round_no >= 2:
                    room.set_commissar_action_mode(player.user_id, "check")
                check_target = _test_pick(rng, room, player)
                if check_target is not None:
                    room.check_player_role(player.user_id, check_target)

    if room.kamikaze_pending_user_id is not None:
        kamikaze = room.get_player(room.kamikaze_pending_user_id)
        if kamikaze is not None and kamikaze.alive:
            target_id = _test_pick(rng, room, kamikaze)
            if target_id is not None:
                room.set_kamikaze_target(kamikaze.user_id, target_id)

    before_alive = {player.user_id for player in room.alive_players()}
    result = room.resolve_night()
    ok, info, eliminated, don_note, don_successor_id, comm_note, comm_successor_id = result
    if not ok:
        raise RuntimeError(f"Ночь не обработана: {info}")

    after_alive = {player.user_id for player in room.alive_players()}
    actual_dead = before_alive - after_alive

    reports = room.pop_night_reports()
    for user_id, lines in reports.items():
        player = room.get_player(user_id)
        if player is None:
            continue
        for line in lines:
            await _test_send(
                bot,
                chat_id,
                f"📨 <b>ТЕСТ-ЛС {player.full_name}:</b> {line}",
            )

    if eliminated:
        for player in eliminated:
            sources = room.night_kill_sources.get(player.user_id, [])
            source_text = f" Источник: {', '.join(sources)}." if sources else ""
            await _test_send(
                bot,
                chat_id,
                f"☠️ <b>ТЕСТ:</b> {player.full_name} погиб ночью. "
                f"Роль: {_test_role_label(player.role)}.{source_text}",
            )
    else:
        await _test_send(bot, chat_id, "🌃 <b>ТЕСТ:</b> этой ночью никто не погиб.")

    if don_note and don_successor_id is not None:
        successor = room.get_player(don_successor_id)
        await _test_send(
            bot,
            chat_id,
            f"🤵🏻 <b>ТЕСТ-наследование Дона:</b> "
            f"{successor.full_name if successor else don_successor_id}",
        )

    if comm_note and comm_successor_id is not None:
        successor = room.get_player(comm_successor_id)
        await _test_send(
            bot,
            chat_id,
            f"🕵️‍♂️ <b>ТЕСТ-наследование Комиссара:</b> "
            f"{successor.full_name if successor else comm_successor_id}",
        )

    # Exercise the current night-to-day last-word path. The actual Telegram
    # bot cannot DM virtual users, so their private submission is represented
    # in the debug feed; publication is still delayed until daytime discussion.
    if eliminated and room.phase != PHASE_FINISHED:
        for player in eliminated:
            if player.user_id in room.afk_killed_user_ids:
                continue
            queued = room.queue_last_words([player])
            if not queued:
                continue
            ok_word, payload = room.consume_last_word(
                player.user_id,
                f"Это был мой последний крик в {room.last_word_death_nights[player.user_id]} ночь.",
            )
            if not ok_word:
                raise RuntimeError(f"Предсмертное не принято для {player.full_name}: {payload}")
            public_text = room.last_word_public_text(player, payload)
            room.queue_last_word_for_day(player.user_id, public_text)
            await _test_send(
                bot,
                chat_id,
                f"🕯 <b>ТЕСТ:</b> {player.full_name} отправил предсмертное. "
                "Оно будет опубликовано следующим днём.",
            )

    if room.phase == PHASE_FINISHED:
        return eliminated

    if actual_dead != {player.user_id for player in eliminated}:
        raise AssertionError(
            f"Несоответствие списка смертей: resolve={sorted(actual_dead)}, "
            f"eliminated={[player.user_id for player in eliminated]}"
        )

    await asyncio.sleep(TEST_GAME_DELAY)
    return eliminated


async def run_telegram_test_game(bot: Bot, chat_id: int, *, player_count: int) -> None:
    rng = random.Random(int(time.time() * 1000) & 0x7FFFFFFF)
    room = GameRoom(chat_id=chat_id, host_id=OWNER_USER_ID)
    room.chat_title = "Telegram Test"

    room.open_registration()
    for user_id in range(1, player_count + 1):
        room.add_player(user_id, f"🤖 Тестовый игрок {user_id}")

    room.assign_roles()

    await _test_send(
        bot,
        chat_id,
        "<b>🧪 TELEGRAM TEST GAME</b>\n"
        f"Виртуальных игроков: <b>{player_count}</b>\n"
        "Игра идёт в реальном Telegram-чате, но игроки виртуальные.\n"
        "Все игровые события ниже — реальные сообщения Telegram-бота.",
    )

    role_lines = [
        f"{player.full_name} — {_test_role_label(player.role)}"
        for player in room.players.values()
    ]
    await _test_send(
        bot,
        chat_id,
        "<b>Раздача ролей:</b>\n" + "\n".join(role_lines),
    )

    for step in range(TEST_GAME_MAX_ROUNDS):
        if room.phase == PHASE_FINISHED:
            break
        if room.phase != PHASE_NIGHT:
            raise RuntimeError(f"Неожиданная фаза перед ночью: {room.phase}")

        await _simulate_one_night(room, bot, chat_id, rng)
        if room.phase == PHASE_FINISHED:
            break

        if room.phase != PHASE_DAY:
            raise RuntimeError(f"После ночи ожидался день, сейчас {room.phase}")

        delayed_last_words = room.pop_last_words_for_day()
        await _test_send(
            bot,
            chat_id,
            f"<b>☀️ ТЕСТ: {room.round_no} день — обсуждение</b>",
        )
        for public_text in delayed_last_words.values():
            await _test_send(bot, chat_id, public_text)

        await asyncio.sleep(TEST_GAME_DELAY)
        await _simulate_one_day(room, bot, chat_id, rng)

    if room.phase != PHASE_FINISHED:
        raise RuntimeError(
            "Игра не закончилась за "
            f"{TEST_GAME_MAX_ROUNDS} раундов. "
            f"Живые: {[(p.full_name, p.role) for p in room.alive_players()]}"
        )

    await _test_send(
        bot,
        chat_id,
        "<b>🏁 ТЕСТОВАЯ ИГРА ЗАВЕРШЕНА</b>\n"
        f"Победитель: <b>{room.winner_team}</b>\n"
        f"Ночей: <b>{room.round_no}</b>\n"
        "Эта партия не была сохранена в настоящую игровую статистику.",
    )


@router.message(Command("testgame"))
async def cmd_testgame(message: Message) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        await message.answer("Эта команда работает только в группе.")
        return
    if not TEST_GAME_ENABLED:
        await message.answer("🧪 Тестовая игра отключена. Включи TEST_GAME_ENABLED=1.")
        return
    if message.from_user is None or not _test_game_allowed(message.from_user.id):
        await message.answer("🧪 Тестовая игра доступна только владельцу/разрешенным тестерам.")
        return
    if message.chat.id in TEST_GAME_TASKS:
        task = TEST_GAME_TASKS[message.chat.id]
        if not task.done():
            await message.answer("🧪 Тестовая игра уже запущена в этом чате.")
            return
        TEST_GAME_TASKS.pop(message.chat.id, None)

    player_count = TEST_GAME_DEFAULT_PLAYERS
    parts = (message.text or "").split()
    if len(parts) >= 2:
        try:
            player_count = int(parts[1])
        except ValueError:
            await message.answer("Использование: /testgame 10")
            return
    if not 4 <= player_count <= 20:
        await message.answer("Количество игроков: от 4 до 20.")
        return

    await message.answer(
        f"🧪 Запускаю тестовую игру на <b>{player_count}</b> виртуальных игроков...",
        parse_mode="HTML",
    )

    async def runner() -> None:
        try:
            await run_telegram_test_game(message.bot, message.chat.id, player_count=player_count)
        except asyncio.CancelledError:
            await message.bot.send_message(message.chat.id, "🧪 Тестовая игра остановлена.")
            raise
        except Exception as exc:
            details = escape("".join(traceback.format_exception(exc)).strip())[-3500:]
            try:
                await message.bot.send_message(
                    message.chat.id,
                    f"<b>❌ ТЕСТОВАЯ ИГРА УПАЛА</b>\n<pre>{details}</pre>",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        finally:
            TEST_GAME_TASKS.pop(message.chat.id, None)

    task = asyncio.create_task(runner())
    TEST_GAME_TASKS[message.chat.id] = task


@router.message(Command("testgame_stop"))
async def cmd_testgame_stop(message: Message) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        await message.answer("Эта команда работает только в группе.")
        return
    if not TEST_GAME_ENABLED or message.from_user is None or not _test_game_allowed(message.from_user.id):
        return

    task = TEST_GAME_TASKS.get(message.chat.id)
    if task is None or task.done():
        await message.answer("🧪 Активной тестовой игры нет.")
        return

    task.cancel()
    await message.answer("🧪 Остановка тестовой игры...")
