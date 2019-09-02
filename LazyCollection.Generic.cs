using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Entity;
using System.Collections.Concurrent;
using System.Threading;

namespace LazyLists
{
    public class LcEventArgs<T> : EventArgs
    {
        public T Value { get; set; }
    }
    public sealed class LazyCollection<T>
    {
        public int BufferCount { get; set; }
        public int UploadCount { get; set; }
        public int Count => State.Count;

        private static Result SetException(Exception error) => new Result(error);
        private static Result<TValue> SetException<TValue>(Exception error) => new Result<TValue>(error);
        private static Result SetResult() => new Result();
        private static Result<TValue> SetResult<TValue>(TValue value) => new Result<TValue>(value);

        public struct Result<TValue>
        {
            public static explicit operator ValueTuple<bool, TValue, Exception>(Result<TValue> result) => (result.IsSuccess, result.Value, result.Error);

            public Result(TValue value) : this()
            {
                IsFailed = false;
                Value = value;
            }

            public Result(Exception error) : this()
            {
                IsFailed = true;
                Error = error ?? throw new ArgumentNullException(nameof(error));
            }

            public bool IsFailed { get; }
            public bool IsSuccess => !IsFailed;
            public TValue Value { get; }
            public Exception Error { get; }
        }

        public struct Result
        {
            public static explicit operator ValueTuple<bool, Exception>(Result result) => (result.IsSuccess, result.Error);
            public Result(Exception error) : this()
            {
                IsFailed = true;
                Error = error ?? throw new ArgumentNullException(nameof(error));
            }

            public bool IsFailed { get; }
            public bool IsSuccess => !IsFailed;
            public Exception Error { get; }
        }

        private struct ValueState
        {
            public ValueState(CancellationTokenSource tokenSource) : this() => TokenSource = tokenSource ?? throw new ArgumentNullException(nameof(tokenSource));

            public ValueState(CancellationTokenSource tokenSource, IQueryable<T> source, Action dispose, ConcurrentDictionary<int, ConcurrentQueue<(EventHandler<LcEventArgs<T>> OnLoad, Action<Exception> OnError)>> handlers, ConcurrentStack<(int From, int Len)> loadQueue, ConcurrentQueue<TaskCompletionSource<bool>> loadSteps, int count, List<(T Value, bool IsLoaded)> list) : this()
            {
                TokenSource = tokenSource ?? throw new ArgumentNullException(nameof(tokenSource));
                Source = source ?? throw new ArgumentNullException(nameof(source));
                Dispose = dispose ?? throw new ArgumentNullException(nameof(dispose));
                Handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
                LoadQueue = loadQueue ?? throw new ArgumentNullException(nameof(loadQueue));
                LoadSteps = loadSteps ?? throw new ArgumentNullException(nameof(loadSteps));
                Count = count;
                List = list ?? throw new ArgumentNullException(nameof(list));
                IsSetted = true;
            }

            public bool IsSetted { get; }
            public CancellationTokenSource TokenSource { get; }
            public IQueryable<T> Source { get; }
            public Action Dispose { get; }
            public ConcurrentDictionary<int, ConcurrentQueue<(EventHandler<LcEventArgs<T>> OnLoad, Action<Exception> OnError)>> Handlers { get; }
            public ConcurrentStack<(int From, int Len)> LoadQueue { get; }
            public ConcurrentQueue<TaskCompletionSource<bool>> LoadSteps { get; }
            public int Count { get; }
            public List<(T Value, bool IsLoaded)> List { get; }
        }

        private CancellationTokenSource _initializeTokenSource;
        private ValueState State { get; set; }

        public LazyCollection()
        {
            _initializeTokenSource = new CancellationTokenSource();
            State = new ValueState(_initializeTokenSource);
        }

        public static async Task<LazyCollection<T>> Create(Type type, IQueryable<T> source, Action dispose)
        {
            var result = await TryCreate(type, source, dispose);
            if (result.IsFailed) throw result.Error;
            return result.Value;
        }
        public static async Task<Result<LazyCollection<T>>> TryCreate(Type type, IQueryable<T> source, Action dispose)
        {
            var item = new LazyCollection<T>();
            var result = await item.TrySet(source, dispose);
            return result.IsFailed ? SetException<LazyCollection<T>>(result.Error) : SetResult(item);
        }
        private readonly object _lockKey = new object();

        public async Task Set(IQueryable<T> source, Action dispose)
        {
            var result = await TrySet(source, dispose).ConfigureAwait(false);
            if (result.IsFailed) throw result.Error;
        }

        public async Task<Result> TrySet(IQueryable<T> source, Action dispose)
        {
            CancellationTokenSource tokenSrc;

            _initializeTokenSource.Cancel();
            tokenSrc = _initializeTokenSource = new CancellationTokenSource();

            if (source == null) return SetException(new ArgumentNullException(nameof(source)));

            var token = tokenSrc.Token;
            var oldState = State;

            var resultCheck = TryCheckCancelInit(token, dispose);
            if (resultCheck.IsFailed) return SetException(resultCheck.Error);

            var resultUpdate = await TryUpdateSrcData(token, source, dispose).ConfigureAwait(false);
            if (resultUpdate.IsFailed) return SetException(resultUpdate.Error);
            var (queue, count, list) = resultUpdate.Value;

            var loadSteps = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var handlers = new ConcurrentDictionary<int, ConcurrentQueue<(EventHandler<LcEventArgs<T>> OnLoad, Action<Exception> OnError)>>();
            var newState = new ValueState(tokenSrc, source, dispose, handlers, queue, loadSteps, count, list);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            LoadMany(newState);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            State = newState;

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            SetCancelAsync(oldState);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            return SetResult();
        }
        private static async Task<Result<(ConcurrentStack<(int From, int Len)>, int, List<(T Value, bool IsLoaded)>)>> TryUpdateSrcData(CancellationToken token, IQueryable<T> source, Action dispose)
        {
            var queue = new ConcurrentStack<(int From, int Len)>();
            int count;
            try
            {
                count = await Task.Run(() => source.Count(), token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                return SetException<(ConcurrentStack<(int From, int Len)>, int, List<(T Value, bool IsLoaded)>)>(new OperationCanceledException());
            }
            var result = TryCheckCancelInit(token, dispose);
            if (result.IsFailed) return SetException<(ConcurrentStack<(int From, int Len)>, int, List<(T Value, bool IsLoaded)>)>(result.Error);

            var list = new (T, bool)[count].ToList();

            return SetResult((queue, count, list));
        }

        private static Result TryCheckCancelInit(CancellationToken token, Action dispose)
        {
            if (token.IsCancellationRequested)
            {
                dispose();
                return SetException(new OperationCanceledException());
            }
            return SetResult();
        }
        private static async Task SetCancelAsync(ValueState state)
        {
            if (!state.IsSetted) return;

            await Task.Run(() =>
            {
                state.TokenSource.Cancel();
                foreach (var promise in state.LoadSteps) promise.TrySetResult(false);
                {
                    var er = new OperationCanceledException();
                    Parallel.ForEach(state.Handlers.Values, item => { foreach (var handler in item) handler.OnError(er); });
                }
                state.Dispose();
            });

        }

        public async Task<T> GetItem(int index)
        {
            var result = await TryGetItem(index);
            if (result.IsFailed) throw result.Error;
            return result.Value;
        }

        public async Task<Result<T>> TryGetItem(int index)
        {
            var state = State;
            if (!state.IsSetted) return SetException<T>(new IndexOutOfRangeException("Collection not initialized"));
            if (state.TokenSource.IsCancellationRequested) return SetException<T>(new OperationCanceledException());
            var promise = new TaskCompletionSource<Result<T>>();

            GetItem(state, index, BufferCount, UploadCount, v => promise.TrySetResult(SetResult(v)), er => promise.TrySetResult(SetException<T>(er)));
            return await promise.Task;
        }
        public void GetItem(int index, Action<T> onLoaded, Action<Exception> onError = null)
        {
            var state = State;
            if (!state.IsSetted)
            {
                onError?.Invoke(new IndexOutOfRangeException("Collection not initialized"));
                return;
            }
            if (state.TokenSource.IsCancellationRequested)
            {
                onError?.Invoke(new OperationCanceledException());
                return;
            }
            GetItem(state, index, BufferCount, UploadCount, onLoaded, onError);
        }

        private static void GetItem(ValueState state, int index, int bufferCount, int uploadCount, Action<T> onLoaded, Action<Exception> onError)
        {
            if (index >= state.Count)
            {
                onError?.Invoke(new ArgumentOutOfRangeException(nameof(index)));
                return;
            }
            if (state.TokenSource.IsCancellationRequested)
            {
                onError?.Invoke(new OperationCanceledException());
                return;
            }
            var (Value, IsLoaded) = state.List[index];

            PassiveLoadNext(state, index, bufferCount, uploadCount);
            PassiveLoadPrevious(state, index, bufferCount, uploadCount);

            if (IsLoaded) onLoaded(Value);
            Load(state, index, bufferCount, onLoaded, onError);
        }

        private static void PassiveLoadNext(ValueState state, int index, int bufferCount, int uploadCount)
        {
            int from = index + bufferCount;
            if (from >= state.Count) return;
            int len = from + bufferCount < state.Count ? bufferCount : state.Count - from;

            PassiveLoad(state, from, len, bufferCount);
        }
        private static void PassiveLoadPrevious(ValueState state, int index, int bufferCount, int uploadCount)
        {
            int from = index - bufferCount;
            if (from < 0) return;
            int len = bufferCount;

            PassiveLoad(state, from, len, uploadCount);
        }

        private static void PassiveLoad(ValueState state, int from, int len, int uploadCount)
        {
            if (!NeedLoadPassive(state, from, len, uploadCount)) return;

            state.LoadQueue.Push((from, len));
            StartLoad(state);
        }
        private static bool NeedLoadPassive(ValueState state, int from, int len, int uploadCount)
        {
            int wontLoad = 0;
            for (int i = from + len - 1; i >= from; i--)
            {
                if (!state.List[i].IsLoaded && !state.LoadQueue.Any(k => k.From <= i && i < k.From + k.Len))
                {
                    if (++wontLoad >= uploadCount) return true;
                };
            }
            return false;
        }
        private static bool NeedLoad(ValueState state, int from, int len, int uploadCount)
        {
            int wontLoad = 0;
            for (int i = from + len - 1; i >= from; i--)
            {
                if (!state.LoadQueue.Any(k => k.From <= i && i < k.From + k.Len))
                {
                    if (++wontLoad >= uploadCount) return true;
                };
            }
            return false;
        }

        private static void Load(ValueState state, int index, int bufferCount, Action<T> onLoaded, Action<Exception> onError)
        {
            if (state.TokenSource.IsCancellationRequested)
            {
                onError(new OperationCanceledException());
                return;
            }

            void Handler(object sender, LcEventArgs<T> value) => onLoaded(value.Value);
            state.Handlers.GetOrAdd(index, HandlersQueueFactory).Enqueue((Handler, onError));
            state.LoadQueue.Push((index, bufferCount));
            StartLoad(state, onError);
        }

        private static void StartLoad(ValueState state, Action<Exception> onError = null)
        {
            state.LoadSteps.TryPeek(out var promise);

            if (state.TokenSource.IsCancellationRequested)
            {
                promise.TrySetCanceled();
                onError?.Invoke(new OperationCanceledException());
            }
            else promise.TrySetResult(true);
        }

        private static ConcurrentQueue<(EventHandler<LcEventArgs<T>> OnLoad, Action<Exception> OnError)> HandlersQueueFactory(int arg) => new ConcurrentQueue<(EventHandler<LcEventArgs<T>> OnLoad, Action<Exception> OnError)>();

        private static async Task LoadMany(ValueState state)
        {
            state.LoadSteps.Enqueue(new TaskCompletionSource<bool>());
            if (state.TokenSource.IsCancellationRequested) return;
            while (true)
            {
                if (state.LoadSteps.TryPeek(out var promise))
                {
                    try
                    {
                        var result = await promise.Task;
                        if (state.TokenSource.IsCancellationRequested || !result) return;
                    }
                    catch (OperationCanceledException) { return; }
                    state.LoadSteps.Enqueue(new TaskCompletionSource<bool>());
                    if (!state.LoadSteps.TryDequeue(out var promise2) || promise != promise2) throw new NotImplementedException();
                    while (!state.LoadQueue.IsEmpty)
                    {
                        if (state.LoadQueue.TryPop(out var item))
                        {
                            if (!await TryLoadAsync(state, item.From, item.Len)) return;
                        }
                    }
                }
                else throw new NotImplementedException();
            }
        }

        private static async Task<bool> TryLoadAsync(ValueState state, int from, int len)
        {
            await LoadInterval(state, from, len);
            return !state.TokenSource.IsCancellationRequested;
        }

        private static async Task LoadInterval(ValueState state, int from, int len)
        {
            if (state.List[from].IsLoaded) return;

            if (Loaded(state, from, len)) return;

            if (from > len)
            {
                from = from - len;
                len += len;
            }
            else
            {
                len += from;
                from = 0;
            }

            System.Diagnostics.Debug.Write($"Loading: {from} - {from + len}\n");

            int i = from;
            var source = state.Source.Skip(from).Take(len);
            List<T> list;
            try
            {
                list = await Task.Run(() => source.ToList(), state.TokenSource.Token);
            }
            catch (Exception er)
            {
                for (int j = from + len - 1; j >= from; j--)
                {
                    OnError(state, j, er);
                }
                return;
            }
            if (state.TokenSource.IsCancellationRequested) return;

            foreach (var item in list)
            {
                state.List[i] = (item, true);
                OnLoaded(state, i++, item);
            }
        }

        private static bool Loaded(ValueState state, int from, int len)
        {
            bool result = true;
            for (int i = from; i >= from; i--)
            {
                var (Value, IsLoaded) = state.List[i];
                result &= IsLoaded;
                if (result) OnLoaded(state, i, Value);
            }
            return result;
        }

        private static void OnLoaded(ValueState state, int i, T item)
        {
            if (state.Handlers.TryGetValue(i, out var handlersQueue))
            {
                while (!handlersQueue.IsEmpty)
                {
                    if (handlersQueue.TryDequeue(out var handler)) handler.OnLoad(state.Source, new LcEventArgs<T> { Value = item });
                }
            }
        }
        private static void OnError(ValueState state, int i, Exception er)
        {
            if (state.Handlers.TryGetValue(i, out var handlersQueue))
            {
                while (!handlersQueue.IsEmpty)
                {
                    if (handlersQueue.TryDequeue(out var handler)) handler.OnError(er);
                }
            }
        }

        public void Dispose()
        {
            var state = State;
            _initializeTokenSource.Cancel();
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            SetCancelAsync(state);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }
    }
}
