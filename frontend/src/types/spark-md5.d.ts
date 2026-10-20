declare module 'spark-md5' {
  class SparkMD5 {
    append(str: string): SparkMD5
    end(raw?: boolean): string
    reset(): SparkMD5
    getState(): SparkMD5.State
    setState(state: SparkMD5.State): SparkMD5
    destroy(): void
    static hash(str: string, raw?: boolean): string
    static hashArray(arr: ArrayLike<number>, raw?: boolean): string
  }

  namespace SparkMD5 {
    interface State {
      buff: Uint8Array
      length: number
      hash: number[]
    }

    class ArrayBuffer {
      append(arr: globalThis.ArrayBuffer): ArrayBuffer
      end(raw?: boolean): string
      reset(): ArrayBuffer
      getState(): State
      setState(state: State): ArrayBuffer
      destroy(): void
      static hash(arr: globalThis.ArrayBuffer, raw?: boolean): string
    }
  }

  export default SparkMD5
  export { SparkMD5 }
}
