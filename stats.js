const steamid = process.argv[2];

if (!steamid) {
    console.log(JSON.stringify({
        ok: false,
        error: "steamid_not_provided"
    }));
    process.exit(1);
}

// Пока ТЕСТОВЫЕ данные
const result = {
    ok: true,
    steamid: steamid,
    level: 5,
    current_xp: 1250,
    xp_needed: 3750,
    progress: 25.0,
    source: "demo"
};

process.stdout.write(JSON.stringify(result));